package core

type FiberMsg interface {
	String() string
	ToDML(Engine) string
	Args() []interface{}
}
