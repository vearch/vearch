// +build amd64,go1.6

#include "textflag.h"

// func cpuid(eaxArg, ecxArg uint32) (eax, ebx, ecx, edx uint32)
TEXT ·cpuid(SB), NOSPLIT, $0-24
	MOVL eaxArg+0(FP), AX
	MOVL ecxArg+4(FP), CX
	CPUID
	MOVL AX, eax+8(FP)
	MOVL BX, ebx+12(FP)
	MOVL CX, ecx+16(FP)
	MOVL DX, edx+20(FP)
	RET

// func EncodeInt(lat, lng float64) uint64
TEXT ·EncodeInt(SB), NOSPLIT, $0
	CMPB ·useAsm(SB), $1
	JNE  fallback

#define LATF	X0
#define LATI	R8
#define LNGF	X1
#define LNGI	R9
#define TEMP	R10
#define GHSH	R11
#define MASK	BX

	MOVSD lat+0(FP), LATF
	MOVSD lng+8(FP), LNGF

	MOVQ $0x5555555555555555, MASK

	MULSD $(0.005555555555555556), LATF
	ADDSD $(1.5), LATF

	MULSD $(0.002777777777777778), LNGF
	ADDSD $(1.5), LNGF

	MOVQ LNGF, LNGI
	SHRQ $20, LNGI

	MOVQ  LATF, LATI
	SHRQ  $20, LATI
	PDEPQ MASK, LATI, GHSH

	PDEPQ MASK, LNGI, TEMP

	SHLQ $1, TEMP
	XORQ TEMP, GHSH

	MOVQ GHSH, ret+16(FP)
	RET

fallback:
	JMP ·encodeInt(SB)
