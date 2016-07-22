package mapreduce

import (
        "fmt"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
        doneChannel := make(chan int, ntasks)

        for i := 0; i < ntasks; i++ {
                go func(task int, phase jobPhase, nios int) {
                        for {
                                // discover idle worker
                                worker := <-mr.registerChannel


                                // create DoTaskArgs
                                var taskInfo DoTaskArgs

                                taskInfo.JobName = mr.jobName
                                taskInfo.File = mr.files[task]
                                taskInfo.Phase = phase
                                taskInfo.TaskNumber = task
                                taskInfo.NumOtherPhase = nios

                                // call workers to handle task via RPC
                                ok := call(worker, "Worker.DoTask", taskInfo, nil)

                                // finished worker reassign status to idle
                                if ok {
                                        doneChannel <- task
                                        mr.registerChannel <- worker
                                        return
                                }
                        }
                }(i, phase, nios)
        }

        for i := 0; i < ntasks; i++ {
                <-doneChannel
        }

	fmt.Printf("Schedule: %v phase done\n", phase)
}
