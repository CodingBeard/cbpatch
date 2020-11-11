package cbpatch

type Patch interface {
	GetAction() string
	GetKey() string
	GetValues() []string
}

type DefaultPatch struct {
	Action string
	Key    string
	Values []string
}

func (d *DefaultPatch) GetAction() string {
	return d.Action
}

func (d *DefaultPatch) GetKey() string {
	return d.Key
}

func (d *DefaultPatch) GetValues() []string {
	return d.Values
}
