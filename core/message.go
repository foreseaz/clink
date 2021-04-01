package core

type FiberMsg interface {
	String() string
	ToDML(Engine) string
	DMLArgs(Engine) [][]interface{}
}
