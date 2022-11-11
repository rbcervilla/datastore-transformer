package transformer

import "cloud.google.com/go/datastore"

var _ datastore.KeyLoader = &Entity{}

// Entity contains key and properties of a datastore entity
//
// Implements datastore.KeyLoader in order to Load and Save
// the entity.
type Entity struct {
	key        *datastore.Key
	Properties map[string]datastore.Property
}

func (e *Entity) LoadKey(k *datastore.Key) error {
	e.key = k
	return nil
}

func (e *Entity) Load(props []datastore.Property) error {
	for _, p := range props {
		e.Properties[p.Name] = p
	}
	return nil
}

func (e *Entity) Save() ([]datastore.Property, error) {
	props := make([]datastore.Property, 0, len(e.Properties))
	for _, p := range e.Properties {
		props = append(props, p)
	}
	return props, nil
}

func newEntity() *Entity {
	return &Entity{
		Properties: make(map[string]datastore.Property),
	}
}
