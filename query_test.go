package db

import "testing"

func TestCreateTable(t *testing.T) {
	// TODO: Update this
	Get().Exec(
		CreateBuilder().CreateTable("users").Build(),
	)
}

func TestSelect(t *testing.T) {
	// TODO: Update this
	Get().Query(
		SelectBuilder().Select("*").From("users").Build(),
	)
}

func TestInsert(t *testing.T) {
	// TODO: Update this
	Get().Exec(
		InsertBuilder().InsertInto("users").Build(),
	)
}

func TestUpdate(t *testing.T) {
	// TODO: Update this
	ub := UpdateBuilder()
	Get().Exec(
		ub.Update("users").Set(ub.Assign("foo", "bar")).Build(),
	)
}

func TestDelete(t *testing.T) {
	// TODO: Update this
	Get().Exec(
		DeleteBuilder().DeleteFrom("users").Build(),
	)
}
