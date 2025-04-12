package db

const (
	OneToOne   = "o2o"
	OneToMany  = "o2m"
	ManyToOne  = "m2o"
	ManyToMany = "m2m"
)

func isSupportedRelationType(relationType string) bool {
	return relationType == OneToOne || relationType == OneToMany || relationType == ManyToOne || relationType == ManyToMany
}
