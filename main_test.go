package kafka

import (
	"os"
	"testing"

	"github.com/deltics/go-logspy"
	"github.com/sirupsen/logrus"
)

func TestMain(m *testing.M) {
	// Confgure the log formatter
	// (TIP: Use a common func to ensure identical formatting
	//        in both test and application logging)
	logrus.SetFormatter(&logrus.JSONFormatter{})

	// Redirect log output to the logspy sink
	logrus.SetOutput(logspy.Sink())

	// Run the tests and set the exit value to the result
	os.Exit(m.Run())
}
