# Build the stuff

go build ./cmd/*.go

go build ./pkg/delay/*.go
go build ./pkg/httperr/*.go
go build ./pkg/httpexpvar/*.go
go build ./pkg/log/*.go
go build ./pkg/pool/*.go
go build ./pkg/tracing/*.go

go build ./services/config/*.go
go build ./services/customer/*.go
go build ./services/driver/*.go
go build ./services/frontend/*.go
go build ./services/route/*.go

go run ./main.go all
