package persistence_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/urjitbhatia/goyaad/pkg/goyaad"
	"github.com/urjitbhatia/goyaad/pkg/persistence"
)

var testBody = []byte("Hello world")

var _ = Describe("Test persistence", func() {
	Context("leveldb persister", func() {
		p := persistence.NewLevelDBPersister("/tmp/test")
		errChan := p.Errors()

		It("persists a goyaad job and then recovers it", func(done Done) {
			defer close(done)

			j := goyaad.NewJobAutoID(time.Now(), testBody)
			data, err := j.GobEncode()
			Expect(err).To(BeNil())
			err = p.Persist(&persistence.Entry{Data: data, Namespace: "simpletest"})
			Expect(err).To(BeNil())

			Expect(errChan).ShouldNot(Receive())

			p.Finalize()

			time.Sleep(time.Second * 2)
			// recovers
			entries, err := p.Recover("simpletest")
			Expect(err).To(BeNil())

			Eventually(entries).Should(Receive(j))
		}, 10.500)
	})
})
