package transformer

import "cloud.google.com/go/datastore"

// Transformer is the interface that wraps methos Transform
//
// Transform performs transformations on the Entity
type Transformer interface {
	Transform(e *Entity)
}

// TransformerFunc type is an adapter to allow the use of functions
// as Transformer
type TransformerFunc func(e *Entity)

func (f TransformerFunc) Transform(e *Entity) {
	f(e)
}

// RemoveField returns a TransformerFunc that remove field of the Entity
func RemoveField(field string) TransformerFunc {
	return func(e *Entity) {
		delete(e.Properties, field)
	}
}

// SetField returns a TransformerFunc that sets a field to the Entity
func SetField(field string, value interface{}, index bool) TransformerFunc {
	return func(e *Entity) {
		f := datastore.Property{
			Name:    field,
			Value:   value,
			NoIndex: !index,
		}
		e.Properties[field] = f
	}
}
