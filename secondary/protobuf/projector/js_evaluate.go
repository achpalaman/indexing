package protobuf

// #cgo CXXFLAGS: -I/Users/aman/.cbdepscache/include -std=c++11
// #cgo LDFLAGS: -L/Users/aman/expt/mr -L/Users/aman/xtr/install/lib -L/usr/lib -lc++ -lv8_libplatform -lv8_libbase -licui18n -licuuc -lv8 -lCGOTRY
// #include "/Users/aman/expt/mr/fork/CGOTRY/Wrapper.h"
// #include <stdlib.h>
// #include <stdio.h>
import "C"

import "unsafe"
import "strconv"
import "github.com/couchbase/indexing/secondary/logging"
import "github.com/couchbase/indexing/secondary/collatejson"

type JSEvaluate struct {
	jsfile *C.char
	E      C.EngineObj
	code   *C.char
}

const (
	STRING C.int = iota
	INT
	FLOAT
	BOOLEANTRUE
	BOOLEANFALSE
	ARRAYSTART
	ARRAYEND
	MAPSTART
	MAPEND
	UNDEFINED
	JSONSTRING
	EMITSTART
	EMITEND
)

const CTerminator = byte(0)

func NewJSEvaluator(file string, code string) *JSEvaluate {
	J := &JSEvaluate{E: C.CreateEngine(2), jsfile: C.CString(file), code: C.CString(code)}
	return J
}

func (J *JSEvaluate) Compile() {
	C.Compile(J.jsfile, J.E, J.code)
}

func (J *JSEvaluate) Run(docid, doc []byte, meta map[string]interface{}, encodeBuf []byte) []byte {
	metaDoc := CreateMeta(meta)
	doc = append(doc, CTerminator)
	response := C.Route(J.E, metaDoc, (*C.char)(unsafe.Pointer(&doc[0])), J.jsfile)
	logging.Infof("DBG: RESPONSE %v", response)
	logging.Infof("DBG: COLLATE VALUE %v", CollateIt(response, encodeBuf))
	return CollateIt(response, encodeBuf)
}

func CollateIt(response C.returnType, encodebuf []byte) []byte {
	var valIndex int
	lengthType := int(C.getLength(response))
	logging.Infof("DBG: Length %v",lengthType)
	if lengthType == 0 {
		return nil
	}
	arrayAddress := uintptr(C.GetTypeArray(response))
	for i := 0; i < lengthType; i++ {
		switch *(*C.int)(unsafe.Pointer(arrayAddress + uintptr(4*i))) {

		case UNDEFINED:
			encodebuf = append(encodebuf, collatejson.TypeMissing, collatejson.Terminator)

		case STRING:
			encodebuf = append(encodebuf, collatejson.TypeString)
			cs := encodeString([]byte(C.GoString(C.getString(response, C.int(valIndex)))), encodebuf[len(encodebuf):])
			encodebuf = encodebuf[:len(encodebuf)+len(cs)]
			valIndex += 1
			encodebuf = append(encodebuf, collatejson.Terminator)

		case INT:
			encodebuf = append(encodebuf, collatejson.TypeNumber)
			intValue := int64(C.getInt(response, C.int(valIndex)))
			var integer collatejson.Integer
			intstr, _ := integer.ConvertToScientificNotation(intValue)
			cs := collatejson.EncodeFloat([]byte(intstr), encodebuf[len(encodebuf):])
			encodebuf = encodebuf[:len(encodebuf)+len(cs)]
			valIndex += 1
			encodebuf = append(encodebuf, collatejson.Terminator)

		case FLOAT:
			encodebuf = append(encodebuf, collatejson.TypeNumber)
			floatValue := float64(C.getFloat(response, C.int(valIndex)))
			cs := collatejson.EncodeFloat([]byte(strconv.FormatFloat(floatValue, 'e', -1, 64)), encodebuf[len(encodebuf):])
			encodebuf = encodebuf[:len(encodebuf)+len(cs)]
			valIndex += 1
			encodebuf = append(encodebuf, collatejson.Terminator)

		case BOOLEANFALSE:
			encodebuf = append(encodebuf, collatejson.TypeFalse, collatejson.Terminator)

		case BOOLEANTRUE:
			encodebuf = append(encodebuf, collatejson.TypeTrue, collatejson.Terminator)

		case ARRAYSTART:
			encodebuf = append(encodebuf, collatejson.TypeArray)

		case ARRAYEND:
			encodebuf = append(encodebuf, collatejson.Terminator)

		case MAPSTART:
			encodebuf = append(encodebuf, collatejson.TypeObj)

		case MAPEND:
			encodebuf = append(encodebuf, collatejson.Terminator)

		case JSONSTRING:
			codec := collatejson.NewCodec(16)
			jsonBytes := []byte(C.GoString(C.getJSON(response, C.int(valIndex))))
			code := make([]byte, 0, 3*len(jsonBytes))
			encoded, err := codec.Encode(jsonBytes, code)
			if err != nil {
				logging.Infof("ERROR in DECODING %v", err)
				break
			}
			valIndex += 1
			encodebuf = append(encodebuf, encoded...)

		}
	}
	encodebuf = append(encodebuf, collatejson.Terminator)
	return append([]byte{collatejson.TypeArray}, encodebuf...)
}

func encodeString(s []byte, code []byte) []byte {
	text := []byte(s)
	for _, x := range text {
		code = append(code, x)
		if x == byte(0) {
			code = append(code, 1)
		}
	}
	code = append(code, byte(0))
	return code
}

func CreateMeta(meta map[string]interface{}) C.struct_metaData {
	metaStruct := C.struct_metaData{}
	for key, value := range meta {
		switch key {
		case "flags":
			metaStruct.flags = C.ulong(value.(uint32))
		case "expiration":
			metaStruct.expiration = C.ulong(value.(uint32))
		case "locktime":
			metaStruct.locktime = C.ulong(value.(uint32))
		case "nru":
			metaStruct.nru = C.int(value.(uint8))
		case "cas":
			metaStruct.cas = C.uint64_t(value.(uint64))
		case "id":
			metaStruct.id = C.CString(value.(string))
		case "byseqno":
			metaStruct.byseqno = C.uint64_t(value.(uint64))
		case "revseqno":
			metaStruct.revseqno = C.uint64_t(value.(uint64))
		}
	}
	return metaStruct
}
