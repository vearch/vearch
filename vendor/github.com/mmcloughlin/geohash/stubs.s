// +build !amd64 !go1.6

// Define NOSPLIT ourselves since "textflag.h" is missing in old Go versions.
#define NOSPLIT	4

TEXT ·EncodeInt(SB), NOSPLIT, $0
	JMP ·encodeInt(SB)
