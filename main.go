package main

import (
	"fmt"

	"github.com/robinkb/cascade-registry/controller"
)

func main() {
	ctrl := controller.NewController()
	err := ctrl.Start()
	if err != nil {
		fmt.Println(err)
	}
}
