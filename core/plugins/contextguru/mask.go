package contextguru

import "crypto/rand"

// credMaskKey masks a caller's credential while this plugin holds it between requests
// (keep-alive). Be precise about what this buys: a heap or core dump, a crash report, a
// `/proc/<pid>/mem` read or a stray `strings` over a snapshot yields masked bytes instead of a
// working key — the accidental-capture class, the realistic one. It does NOT stop an attacker with
// code execution in this process: the key is in the same address space. It is obfuscation at rest,
// not encryption.
//
// Private to this plugin: credential custody is each host's own business, so this is not shared
// with the engine's core or with the proxy host.
var credMaskKey = func() []byte {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("context-guru: no entropy for the credential mask: " + err.Error())
	}
	return b
}()

// credMask masks or unmasks in place — XOR is its own inverse, so one function does both.
func credMask(b []byte) {
	for i := range b {
		b[i] ^= credMaskKey[i%len(credMaskKey)]
	}
}

// credZero overwrites a buffer. Dropping the reference is not enough: the bytes sit in the heap
// until a collection that may never come, and a dump taken in between still has them.
func credZero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// heldHeader is one auth header held masked. The NAME is not a secret; the value is.
type heldHeader struct {
	name string
	val  []byte
}

func (h heldHeader) clone() heldHeader {
	return heldHeader{name: h.name, val: append([]byte(nil), h.val...)}
}

func zeroHeaders(hs []heldHeader) {
	for _, h := range hs {
		credZero(h.val)
	}
}
