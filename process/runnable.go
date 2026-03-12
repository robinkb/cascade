package process

// Runnable represents a process that when started will run indefinitely until stopped.
type Runnable interface {
	// Name returns a human-readable identifier for the process.
	Name() string
	// Run is a blocking method that runs the process.
	// It returns an error when the process is stopped unexpectedly.
	Run() error
	// Shutdown initiates a graceful shutdown of the process.
	// It returns an error when the process is not able to stop correctly,
	// or when stopping the process times out.
	Shutdown() error
}
