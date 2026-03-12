package server

import "net/http"

func NewResponseWriter(w http.ResponseWriter) *ResponseWriter {
	return &ResponseWriter{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
	}
}

type ResponseWriter struct {
	http.ResponseWriter
	statusCode    int
	headerWritten bool
}

func (w *ResponseWriter) Write(b []byte) (int, error) {
	w.headerWritten = true
	return w.ResponseWriter.Write(b)
}

func (w *ResponseWriter) WriteHeader(statusCode int) {
	w.ResponseWriter.WriteHeader(statusCode)

	if !w.headerWritten {
		w.statusCode = statusCode
		w.headerWritten = true
	}
}

func (w *ResponseWriter) Code() int {
	if !w.headerWritten {
		panic("attempt to read status code before headers have been written")
	}
	return w.statusCode
}

func (w *ResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
