// Command configui serves the setup page for the Inbound Connector and the Outbound Connector: see
// the configui package in nexus-connector-shared. connector.go says what is particular to this repo.
//
//	go run ./configui                          # then open http://localhost:8090
//	go run ./configui --addr localhost:9000
package main

import (
	"log"
	"os"

	"github.com/temporalio/nexus-connector-kafka/nexus-connector-shared/configui"
)

func main() {
	if err := configui.Run(pageTitle, os.Args[1:], programs); err != nil {
		log.Fatal(err)
	}
}
