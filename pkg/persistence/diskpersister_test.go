package persistence_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/urjitbhatia/goyaad/pkg/goyaad"
	"github.com/urjitbhatia/goyaad/pkg/persistence"
)

var testBody = []byte("Hello world")

var _ = Describe("Test disk persistence", func() {
	Context("writes to disk", func() {
		p := persistence.NewDiskPersister()

		It("for a give entry", func() {
			j := goyaad.NewJobAutoID(time.Now(), testBody)
			err := p.Persist(&persistence.Entry{Data: j, Namespace: "test"})
			Expect(err).To(BeNil())
		})
	})
})
