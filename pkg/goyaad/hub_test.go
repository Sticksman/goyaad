package goyaad_test

import (
	"math/rand"
	"os"
	"path"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/urjitbhatia/goyaad/pkg/goyaad"
	"github.com/urjitbhatia/goyaad/pkg/persistence"
)

var dataDir = path.Join(os.TempDir(), "goyaadtest")
var persister persistence.Persister

var _ = Describe("Test hub", func() {

	BeforeEach(func() {
		persister = persistence.NewLevelDBPersister(dataDir)
		Expect(persister.ResetDataDir()).To(BeNil())
	})

	It("can create a hub", func() {
		// A hub with 10ms spokes
		h := NewHub(time.Millisecond*10, persister)
		Expect(h.PendingJobsCount()).To(Equal(0))
	})

	It("accepts jobs with random times and random spoke durations into a hub", func() {
		for i := 0; i < 50; i++ {
			h := NewHub(time.Second*time.Duration(rand.Intn(2999)+1), persister)

			j := NewJobAutoID(time.Now().Add(time.Millisecond*time.Duration(rand.Intn(999999))), nil)
			h.AddJob(j)

			Expect(h.PendingJobsCount()).To(Equal(1))
		}
	})

	It("walks job from a hub in proper order - with timeout", func(done Done) {
		defer close(done)

		// hub with spokes spanning  3000 nanosec (Faster for testing)
		h := NewHub(time.Nanosecond*3000, persister)

		// Add a jobs with a random trigger time in the future - max 9999 nanosec
		jobs := [1000]*Job{}
		for i := 0; i < len(jobs); i++ {
			// Some jobs could already be in the past
			triggerAt := time.Now().Add(time.Nanosecond * time.Duration(rand.Intn(9999)))
			if rand.Float32() <= 0.2 {
				triggerAt = time.Now().Add(time.Nanosecond * time.Duration(-1*rand.Intn(9999)))
			}

			j := NewJobAutoID(triggerAt, nil)

			jobs[i] = j
		}

		// Shuffle jobs
		rand.Shuffle(len(jobs), func(i, j int) {
			jobs[i], jobs[j] = jobs[j], jobs[i]
		})

		// Add all of them
		for i, j := range jobs {
			h.AddJob(j)
			Expect(h.PendingJobsCount()).To(Equal(i + 1))
		}

		// Walk should return all jobs in global order
		walked := []*Job{}
		for h.PendingJobsCount() > 0 {
			walked = append(walked, h.Next())
		}

		// Expect correct order
		var prev *Job = nil
		for _, j := range walked {
			if prev != nil {
				Expect(prev.TriggerAt().Before(j.TriggerAt()))
			}
			prev = j
		}

		Expect(h.PendingJobsCount()).To(Equal(0))

	}, 1.500)

	It("Persists and recovers from disk", func(done Done) {
		defer close(done)

		h := NewHub(time.Nanosecond*3000, persister)

		// Add a jobs with a random trigger time in the future - max 9999 nanosec
		jobs := [1000]*Job{}
		for i := 0; i < len(jobs); i++ {
			// Some jobs could already be in the past
			triggerAt := time.Now().Add(time.Nanosecond * time.Duration(rand.Intn(9999)))
			if rand.Float32() <= 0.2 {
				triggerAt = time.Now().Add(time.Nanosecond * time.Duration(-1*rand.Intn(9999)))
			}

			j := NewJobAutoID(triggerAt, nil)

			jobs[i] = j
		}

		jobMap := make(map[string]*Job, len(jobs))
		// Add all of them
		for i, j := range jobs {
			h.AddJob(j)
			jobMap[j.ID()] = j
			Expect(h.PendingJobsCount()).To(Equal(i + 1))
		}

		// Reserve some jobs
		Expect(h.Next()).ToNot(BeNil())
		Expect(h.Next()).ToNot(BeNil())
		Expect(h.Next()).ToNot(BeNil())

		// Persist
		persistErrs := h.Persist()

		// if any errors pop up, fail the test
		for e := range persistErrs {
			Fail("Persist failed due to error: " + e.Error())
		}

		entries, err := persister.Recover("job")
		Expect(err).To(BeNil())
		counter := 0
		for range entries {
			counter++
		}

		Expect(counter).To(Equal(h.PendingJobsCount() + h.ReservedJobsCount()))
	}, 15)

})
