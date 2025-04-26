package model

type Column[Model any, ColType comparable] struct {
	Name string
	Func func(m *Model) ColType
}

type Definition[Model any, Schema any] struct {
	Table  string
	Schema Schema
}

func Define[Model any, Schema any](definition Definition[Model, Schema]) *Model {
	var model Model
	if definition.Table == "" {
		panic("table name is required")
	}
	return &model
}

func Col[Model any, ColType comparable](name string, funcPtr func(m *Model) ColType) *Column[Model, ColType] {
	return &Column[Model, ColType]{
		Name: name,
		Func: funcPtr,
	}
}

func AutoIncrement[Model any, ColType comparable](name string, funcPtr func(m *Model) ColType) *Column[Model, ColType] {
	col := &Column[Model, ColType]{
		Name: name,
		Func: funcPtr,
	}

	return col
}
