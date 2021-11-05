// +build amd64,go1.6

package geohash

// useAsm flag determines whether the assembly version of EncodeInt will be
// used. By Default we fall back to encodeInt.
var useAsm bool

// cpuid executes the CPUID instruction to obtain processor identification and
// feature information.
func cpuid(eaxArg, ecxArg uint32) (eax, ebx, ecx, edx uint32)

// hasBMI2 returns whether the CPU supports Bit Manipulation Instruction Set
// 2.
func hasBMI2() bool {
	_, ebx, _, _ := cpuid(7, 0)
	return ebx&(1<<8) != 0
}

// init determines whether to use assembly version by performing CPU feature
// check.
func init() {
	useAsm = hasBMI2()
}
