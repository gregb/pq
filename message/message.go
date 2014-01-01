package message

// message codes from http://www.postgresql.org/docs/9.2/static/protocol-message-formats.html
type Backend byte
type Frontend byte

const (
	// Backend messages.  received from server
	NotificationResponse Backend = 'A'
	CommandComplete      Backend = 'C'
	DataRow              Backend = 'D'
	Error                Backend = 'E'
	KeyData              Backend = 'K'
	Authenticate         Backend = 'R'
	ParameterStatus      Backend = 'S'
	RowDescription       Backend = 'T'
	ParameterDescription Backend = 't'
	NoData               Backend = 'n'
	Notice               Backend = 'N'
	ReadyForQuery        Backend = 'Z'
	ParseComplete        Backend = '1'
	BindComplete         Backend = '2'
	CloseComplete        Backend = '3'
)

const (
	// Frontend messages.  sent to server
	Bind      Frontend = 'B'
	Close     Frontend = 'C'
	Describe  Frontend = 'D'
	Execute   Frontend = 'E'
	Parse     Frontend = 'P'
	Password  Frontend = 'p'
	Query     Frontend = 'Q'
	Sync      Frontend = 'S'
	Terminate Frontend = 'X'
)
