package metadata

type TableTitle struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

type DatabasesMeta struct {
	Name   string `json:"name"`
	Engine string `json:"engine"`
	Query  string `json:"query"`
}

type FunctionsMeta struct {
	Name        string `json:"name"`
	CreateQuery string `json:"create_query"`
}

type MutationMetadata struct {
	MutationId string `json:"mutation_id" ch:"mutation_id"`
	Command    string `json:"command" ch:"command"`
}

type Part struct {
	Name           string `json:"name"`
	Required       bool   `json:"required,omitempty"`
	RebalancedDisk string `json:"rebalanced_disk,omitempty"`
}

type SplitPartFiles struct {
	Prefix string
	Files  []string
}
