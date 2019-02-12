package idx

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unsafe"

	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
	"github.com/tinylib/msgp/msgp"
)

//go:generate msgp

func init() {
	msgp.RegisterExtension(95, func() msgp.Extension { return &MetricName{} })
	msgp.RegisterExtension(90, func() msgp.Extension { return &TagKeyValues{} })
}

type mType uint8

// MType represented by a uint8
const (
	MTypeUndefined mType = iota
	MTypeGauge
	MTypeRate
	MTypeCount
	MTypeCounter
	MTypeTimestamp
)

// MetricName stores the name as a []uintptr to strings interned in an object store.
// Each word is stored as a separate interned string without the '.'. The whole name
// can be retrieved by calling MetricName.String()
type MetricName struct {
	nodes []uintptr
}

// Nodes returns the []uintptr of interned string addresses
// for the MetricName
func (mn *MetricName) Nodes() []uintptr {
	return mn.nodes
}

// String returns the full MetricName as a string
// using data interned in the object store
func (mn *MetricName) String() string {
	if len(mn.nodes) == 0 {
		return ""
	}

	bld := strings.Builder{}
	return mn.string(&bld)
}

func (mn *MetricName) string(bld *strings.Builder) string {
	// get []int of the lengths of all of the mn.Nodes
	lns, ok := IdxIntern.Len(mn.nodes)
	if !ok {
		internError.Inc()
		log.Error("idx: Failed to retrieve length of strings from interning library")
		return ""
	}

	// should be faster than calling IdxIntern.SetString in a tight loop
	var tmpSz string
	szHeader := (*reflect.StringHeader)(unsafe.Pointer(&tmpSz))
	first, _ := IdxIntern.GetNoRefCntString(mn.nodes[0])
	bld.WriteString(first)
	for idx, nodePtr := range mn.nodes[1:] {
		szHeader.Data = nodePtr
		szHeader.Len = lns[idx+1]
		bld.WriteString(".")
		bld.WriteString(tmpSz)
	}

	return bld.String()
}

// setMetricName interns the MetricName in an
// object store and stores the addresses of those strings
// in MetricName.nodes
func (mn *MetricName) setMetricName(name string) {
	nodes := strings.Split(name, ".")
	mn.nodes = make([]uintptr, len(nodes))
	for i, node := range nodes {
		// TODO: add error checking? Fail somehow
		nodePtr, err := IdxIntern.AddOrGet([]byte(node))
		if err != nil {
			log.Error("idx: Failed to acquire interned string for node name: ", err)
			internError.Inc()
		}
		mn.nodes[i] = nodePtr
	}
}

// ExtensionType is required to use custom marshaling as an extension
// with msgp
func (mn *MetricName) ExtensionType() int8 {
	return 95
}

// Len is required to use custom marshaling as an extension
// with msgp
func (mn *MetricName) Len() int {
	return len(mn.String())
}

// MarshalBinaryTo is required to use custom marshaling as an extension
// in msgp
func (mn *MetricName) MarshalBinaryTo(b []byte) error {
	copy(b, []byte(mn.String()))
	return nil
}

// UnmarshalBinary is required to use custom marshaling as an extension
// in msgp
func (mn *MetricName) UnmarshalBinary(b []byte) error {
	mn.setMetricName(string(b))
	return nil
}

// TagKeyValue stores a Key/Value pair. The strings
// are interned in an object store before they are assigned.
type TagKeyValue struct {
	Key   uintptr
	Value uintptr
}

// String returns a Key/Value pair in the form of
// 'key=value'
func (t *TagKeyValue) String() string {
	bld := strings.Builder{}

	key, err := IdxIntern.GetNoRefCntString(t.Key)
	if err != nil {
		log.Error("idx: Failed to retrieve interned tag key: ", err)
		internError.Inc()
	}
	val, err := IdxIntern.GetNoRefCntString(t.Value)
	if err != nil {
		log.Error("idx: Failed to retrieve interned tag value: ", err)
		internError.Inc()
	}

	bld.WriteString(key)
	bld.WriteString("=")
	bld.WriteString(val)

	return bld.String()
}

// TagKeyValues stores a slice of all of the Tag Key/Value pair combinations for a MetricDefinition
type TagKeyValues struct {
	KeyValues []TagKeyValue
}

// Strings returns a slice containing all of the Tag Key/Value pair combinations for a MetricDefinition.
// Each item in the slice is in the form of 'key=value'
func (t *TagKeyValues) Strings() []string {
	tags := make([]string, len((*t).KeyValues))
	for i, tag := range (*t).KeyValues {
		tags[i] = tag.String()
	}
	return tags
}

func (t *TagKeyValues) ExtensionType() int8 {
	return 90
}

func (t *TagKeyValues) Len() int {
	var total int
	for _, kv := range t.Strings() {
		total += len(kv) + 1
	}
	return total
}

func (t *TagKeyValues) MarshalBinaryTo(b []byte) error {
	for idx, kv := range t.Strings() {
		b = append(b, []byte(kv)...)
		b = append(b, ';')
	}
	return nil
}

func (t *TagKeyValues) UnmarshalBinary(b []byte) error {
	tags := strings.Split(string(b), ";")
	tmp := make([]TagKeyValue, len(tags))
	t.KeyValues = tmp
	for i, tag := range tags {
		if strings.Contains(tag, ";") {
			log.Errorf("idx: Tag %q has an invalid format, ignoring", tag)
			invalidTag.Inc()
			continue
		}
		eqPos := strings.Index(tag, "=")
		if eqPos < 0 {
			log.Errorf("idx: Tag %q has an invalid format, ignoring", tag)
			invalidTag.Inc()
			continue
		}
		key, err := IdxIntern.AddOrGet([]byte(tag[:eqPos]))
		if err != nil {
			log.Errorf("idx: Failed to intern tag %q, %v", tag, err)
			internError.Inc()
			continue
		} else {
			(*t).KeyValues[i].Key = key
		}

		value, err := IdxIntern.AddOrGet([]byte(tag[eqPos+1:]))
		if err != nil {
			log.Errorf("idx: Failed to intern tag %q, %v", tag, err)
			internError.Inc()
			continue
		} else {
			(*t).KeyValues[i].Value = value
		}
	}
	return nil
}

// MetricDefinition stores information which identifies a single metric
type MetricDefinition struct {
	Id    schema.MKey
	OrgId uint32
	// using custom marshalling for MetricName
	// if there is another way we should explore that
	Name       MetricName `msg:"name,extension"`
	Interval   int
	Unit       string
	mtype      mType
	Tags       TagKeyValues `msg:"tagkeyvalues,extension"`
	LastUpdate int64
	Partition  int32
}

// NameWithTags returns a string version of the MetricDefinition's name with
// all of its tags in the form of 'name;key1=value1;key2=value2;key3=value3'
func (md *MetricDefinition) NameWithTags() string {
	bld := strings.Builder{}

	md.Name.string(&bld)
	sort.Slice(md.Tags, func(i, j int) bool {
		return md.Tags.KeyValues[i].Key > md.Tags.KeyValues[j].Key
	})
	for _, tag := range md.Tags.KeyValues {
		key, err := IdxIntern.GetNoRefCntString(tag.Key)
		if err != nil {
			log.Error("idx: Failed to retrieve interned tag key: ", err)
			internError.Inc()
			continue
		}
		if key == "name" {
			continue
		}
		bld.WriteString(";")
		bld.WriteString(tag.String())
	}
	return bld.String()
}

// SetMType translates a string into a uint8 which is used to store
// the actual metric type. Valid values are 'gauge', 'rate', 'count',
// 'counter', and 'timestamp'.
func (md *MetricDefinition) SetMType(mtype string) {
	switch mtype {
	case "gauge":
		md.mtype = MTypeGauge
	case "rate":
		md.mtype = MTypeRate
	case "count":
		md.mtype = MTypeCount
	case "counter":
		md.mtype = MTypeCounter
	case "timestamp":
		md.mtype = MTypeTimestamp
	default:
		// for values "" and other unknown/corrupted values
		md.mtype = MTypeUndefined
	}
}

// Mtype returns a string version of the current MType
func (md *MetricDefinition) Mtype() string {
	switch md.mtype {
	case MTypeGauge:
		return "gauge"
	case MTypeRate:
		return "rate"
	case MTypeCount:
		return "count"
	case MTypeCounter:
		return "counter"
	case MTypeTimestamp:
		return "timestamp"
	default:
		// case of MTypeUndefined and also default for unknown/corrupted values
		return ""
	}
}

// SetUnit takes a string, interns it in an object store
// and then uses it to store the unit.
func (md *MetricDefinition) SetUnit(unit string) {
	sz, err := IdxIntern.AddOrGetString([]byte(unit))
	if err != nil {
		log.Errorf("idx: Failed to intern Unit %v. %v", unit, err)
		internError.Inc()
		md.Unit = unit
		return
	}
	md.Unit = sz
}

// SetMetricName interns the MetricName in an
// object store and stores the addresses of those strings
// in MetricName.nodes
func (md *MetricDefinition) SetMetricName(name string) {
	nodes := strings.Split(name, ".")
	md.Name.nodes = make([]uintptr, len(nodes))
	for i, node := range nodes {
		// TODO: add error checking? Fail somehow
		nodePtr, err := IdxIntern.AddOrGet([]byte(node))
		if err != nil {
			log.Errorf("idx: Failed to intern word in MetricName: %v, %v", node, err)
		}
		md.Name.nodes[i] = nodePtr
	}
}

// SetTags takes a []string which should contain Key/Value pairs
// in the form of 'key=value'. It splits up the Key and Value for each
// item, interns them in the object store, and creates a TagKeyValue
// for them. It then stores all of these in Tags.
//
// The items in the input argument should not contain ';'. Each item
// is a separate Key/Value pair. Do not combine multiple Key/Value pairs
// into a single index in the []string.
func (md *MetricDefinition) SetTags(tags []string) {
	md.Tags.KeyValues = make([]TagKeyValue, len(tags))
	sort.Strings(tags)
	for i, tag := range tags {
		if strings.Contains(tag, ";") {
			log.Errorf("idx: Tag %q has an invalid format, ignoring", tag)
			invalidTag.Inc()
			continue
		}
		eqPos := strings.Index(tag, "=")
		if eqPos < 0 {
			log.Errorf("idx: Tag %q has an invalid format, ignoring", tag)
			invalidTag.Inc()
			continue
		}
		key, err := IdxIntern.AddOrGet([]byte(tag[:eqPos]))
		if err != nil {
			log.Errorf("idx: Failed to intern tag %q, %v", tag, err)
			internError.Inc()
			continue
		} else {
			md.Tags.KeyValues[i].Key = key
		}

		value, err := IdxIntern.AddOrGet([]byte(tag[eqPos+1:]))
		if err != nil {
			log.Errorf("idx: Failed to intern tag %q, %v", tag, err)
			internError.Inc()
			continue
		} else {
			md.Tags.KeyValues[i].Value = value
		}
	}
}

// SetId creates and sets the MKey which identifies a metric
func (md *MetricDefinition) SetId() {
	sort.Slice(md.Tags, func(i, j int) bool {
		return md.Tags.KeyValues[i].Key > md.Tags.KeyValues[j].Key
	})
	buffer := bytes.NewBufferString(md.Name.String())
	buffer.WriteByte(0)
	buffer.WriteString(md.Unit)
	buffer.WriteByte(0)
	buffer.WriteString(md.Mtype())
	buffer.WriteByte(0)
	fmt.Fprintf(buffer, "%d", md.Interval)

	for _, t := range md.Tags.KeyValues {
		key, err := IdxIntern.GetNoRefCntString(t.Key)
		if err != nil {
			log.Error("idx: Failed to retrieve interned tag key: ", err)
			internError.Inc()
			continue
		}
		if key == "name" {
			continue
		}

		buffer.WriteByte(0)
		buffer.WriteString(t.String())
	}

	md.Id = schema.MKey{
		Key: md5.Sum(buffer.Bytes()),
		Org: uint32(md.OrgId),
	}
}

// MetricDefinitionFromMetricDataWithMKey takes an MKey and MetricData and returns a MetricDefinition
// based on them.
func MetricDefinitionFromMetricDataWithMKey(mkey schema.MKey, d *schema.MetricData) *MetricDefinition {
	md := &MetricDefinition{
		Id:         mkey,
		OrgId:      uint32(d.OrgId),
		Interval:   d.Interval,
		LastUpdate: d.Time,
	}

	md.SetMetricName(d.Name)
	md.SetUnit(d.Unit)
	md.SetMType(d.Mtype)
	md.SetTags(d.Tags)

	return md
}

// MetricDefinitionFromMetricData takes a MetricData, attempts to generate an MKey for it,
// and returns a MetricDefinition upon success. On failure it returns an error
func MetricDefinitionFromMetricData(d *schema.MetricData) (*MetricDefinition, error) {
	mkey, err := schema.MKeyFromString(d.Id)
	if err != nil {
		return nil, fmt.Errorf("idx: Error parsing ID: %s", err)
	}

	return MetricDefinitionFromMetricDataWithMKey(mkey, d), nil
}