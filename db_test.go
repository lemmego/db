package db

import "testing"

func TestAddConnection(t *testing.T) {
	DM().Add("default", NewConnection(&Config{
		ConnName: "",
		Driver:   "",
		Host:     "",
		Port:     0,
		User:     "",
		Password: "",
		Database: "",
		Params:   "",
	}))
}
