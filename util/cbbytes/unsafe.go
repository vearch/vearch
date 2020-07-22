package cbbytes

// ByteToString convert bytes to string
// Note: string and slice share a block of memory, for scenarios where slice does not change
func ByteToString(b []byte) string {
	return string(b)
	//bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	//sh := reflect.StringHeader{
	//	Data: bh.Data,
	//	Len:  bh.Len,
	//}
	//return *(*string)(unsafe.Pointer(&sh))
}

// StringToByte convert string to bytes
// Note: string and slice share a block of memory, for scenarios where slice does not change
func StringToByte(s string) []byte {
	return []byte(s)
	//sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	//bh := reflect.SliceHeader{
	//	Data: sh.Data,
	//	Len:  sh.Len,
	//	Cap:  sh.Len,
	//}
	//return *(*[]byte)(unsafe.Pointer(&bh))
}
