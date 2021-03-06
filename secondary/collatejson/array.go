package collatejson

import "errors"

var ErrNotAnArray = errors.New("not an array")
var ErrLenPrefixUnsupported = errors.New("arrayLenPrefix is unsupported")

func (codec *Codec) ExplodeArray(code []byte, tmp []byte) ([][]byte, error) {
	var err error

	array := make([][]byte, 0)

	if codec.arrayLenPrefix {
		return nil, ErrLenPrefixUnsupported
	}

	if code[0] != TypeArray {
		return nil, ErrNotAnArray
	}

	code = code[1:]
	elemBuf := code
	for code[0] != Terminator {
		_, code, err = codec.code2json(code, tmp)
		if err != nil {
			break
		}

		if size := len(elemBuf) - len(code); size > 0 {
			array = append(array, elemBuf[:size])
			elemBuf = code
		}
	}

	return array, err
}

// Explodes an encoded array, returns encoded parts as well as
// decoded parts
func (codec *Codec) ExplodeArray2(code []byte, tmp, decbuf []byte, cktmp, dktmp [][]byte) ([][]byte, [][]byte, error) {
	var err error
	var text []byte

	if codec.arrayLenPrefix {
		return nil, nil, ErrLenPrefixUnsupported
	}

	if code[0] != TypeArray {
		return nil, nil, ErrNotAnArray
	}

	code = code[1:]
	elemBuf := code

	pos := 0
	for code[0] != Terminator {
		text, code, err = codec.code2json(code, tmp)
		if err != nil {
			break
		}

		if dktmp != nil {
			copy(decbuf, text)
			dktmp[pos] = decbuf[:len(text)]
			decbuf = decbuf[len(text):]
		}

		if size := len(elemBuf) - len(code); size > 0 {
			cktmp[pos] = elemBuf[:size]
			elemBuf = code
		}
		pos++
	}

	return cktmp, dktmp, err
}

func (codec *Codec) JoinArray(vals [][]byte, code []byte) ([]byte, error) {
	if codec.arrayLenPrefix {
		return nil, ErrLenPrefixUnsupported
	}

	code = append(code, TypeArray)
	for _, val := range vals {
		code = append(code, val...)
	}
	code = append(code, Terminator)

	return code, nil
}
