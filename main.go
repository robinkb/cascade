package main

import (
	"fmt"
	"os"

	"github.com/robinkb/cascade-registry/controller"
)

func main() {
	ctrl := controller.NewController()
	err := ctrl.Start()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
