package db

// ConditionFunc is a function that returns a string representing a condition.
// It proxies the calls to the original condition functions.
type ConditionFunc func(builder Builder) string

// Equal is used to construct the expression "field = value".
func Equal(field string, value interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).Equal(field, value)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).Equal(field, value)
		case *BuilderDelete:
			return builder.(*BuilderDelete).Equal(field, value)
		}

		return ""
	}
}

// E is an alias of Equal.
func E(field string, value interface{}) ConditionFunc {
	return Equal(field, value)
}

// EQ is an alias of Equal.
func EQ(field string, value interface{}) ConditionFunc {
	return Equal(field, value)
}

// NotEqual is used to construct the expression "field <> value".
func NotEqual(field string, value interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).NotEqual(field, value)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).NotEqual(field, value)
		case *BuilderDelete:
			return builder.(*BuilderDelete).NotEqual(field, value)
		}

		return ""
	}
}

// NE is an alias of NotEqual.
func NE(field string, value interface{}) ConditionFunc {
	return NotEqual(field, value)
}

// NEQ is an alias of NotEqual.
func NEQ(field string, value interface{}) ConditionFunc {
	return NotEqual(field, value)
}

// GreaterThan is used to construct the expression "field > value".
func GreaterThan(field string, value interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).GreaterThan(field, value)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).GreaterThan(field, value)
		case *BuilderDelete:
			return builder.(*BuilderDelete).GreaterThan(field, value)
		}

		return ""
	}
}

// G is an alias of GreaterThan.
func G(field string, value interface{}) ConditionFunc {
	return GreaterThan(field, value)
}

// GT is an alias of GreaterThan.
func GT(field string, value interface{}) ConditionFunc {
	return GreaterThan(field, value)
}

// GreaterEqualThan is used to construct the expression "field >= value".
func GreaterEqualThan(field string, value interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).GreaterEqualThan(field, value)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).GreaterEqualThan(field, value)
		case *BuilderDelete:
			return builder.(*BuilderDelete).GreaterEqualThan(field, value)
		}

		return ""
	}
}

// GE is an alias of GreaterEqualThan.
func GE(field string, value interface{}) ConditionFunc {
	return GreaterEqualThan(field, value)
}

// GTE is an alias of GreaterEqualThan.
func GTE(field string, value interface{}) ConditionFunc {
	return GreaterEqualThan(field, value)
}

// LessThan is used to construct the expression "field < value".
func LessThan(field string, value interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).LessThan(field, value)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).LessThan(field, value)
		case *BuilderDelete:
			return builder.(*BuilderDelete).LessThan(field, value)
		}

		return ""
	}
}

// L is an alias of LessThan.
func L(field string, value interface{}) ConditionFunc {
	return LessThan(field, value)
}

// LT is an alias of LessThan.
func LT(field string, value interface{}) ConditionFunc {
	return LessThan(field, value)
}

// LessEqualThan is used to construct the expression "field <= value".
func LessEqualThan(field string, value interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).LessEqualThan(field, value)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).LessEqualThan(field, value)
		case *BuilderDelete:
			return builder.(*BuilderDelete).LessEqualThan(field, value)
		}

		return ""
	}
}

// LE is an alias of LessEqualThan.
func LE(field string, value interface{}) ConditionFunc {
	return LessEqualThan(field, value)
}

// LTE is an alias of LessEqualThan.
func LTE(field string, value interface{}) ConditionFunc {
	return LessEqualThan(field, value)
}

// In is used to construct the expression "field IN (value...)".
func In(field string, values ...interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).In(field, values...)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).In(field, values...)
		case *BuilderDelete:
			return builder.(*BuilderDelete).In(field, values...)
		}

		return ""
	}
}

// NotIn is used to construct the expression "field NOT IN (value...)".
func NotIn(field string, values ...interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).NotIn(field, values...)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).NotIn(field, values...)
		case *BuilderDelete:
			return builder.(*BuilderDelete).NotIn(field, values...)
		}

		return ""
	}
}

// Like is used to construct the expression "field LIKE value".
func Like(field string, value interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).Like(field, value)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).Like(field, value)
		case *BuilderDelete:
			return builder.(*BuilderDelete).Like(field, value)
		}

		return ""
	}
}

// ILike is used to construct the expression "field ILIKE value".
//
// When the database system does not support the ILIKE operator,
// the ILike method will return "LOWER(field) LIKE LOWER(value)"
// to simulate the behavior of the ILIKE operator.
func ILike(field string, value interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).ILike(field, value)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).ILike(field, value)
		case *BuilderDelete:
			return builder.(*BuilderDelete).ILike(field, value)
		}

		return ""
	}
}

// NotLike is used to construct the expression "field NOT LIKE value".
func NotLike(field string, value interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).NotLike(field, value)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).NotLike(field, value)
		case *BuilderDelete:
			return builder.(*BuilderDelete).NotLike(field, value)
		}

		return ""
	}
}

// NotILike is used to construct the expression "field NOT ILIKE value".
//
// When the database system does not support the ILIKE operator,
// the NotILike method will return "LOWER(field) NOT LIKE LOWER(value)"
// to simulate the behavior of the ILIKE operator.
func NotILike(field string, value interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).NotILike(field, value)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).NotILike(field, value)
		case *BuilderDelete:
			return builder.(*BuilderDelete).NotILike(field, value)
		}

		return ""
	}
}

// IsNull is used to construct the expression "field IS NULL".
func IsNull(field string) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).IsNull(field)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).IsNull(field)
		case *BuilderDelete:
			return builder.(*BuilderDelete).IsNull(field)
		}

		return ""
	}
}

// IsNotNull is used to construct the expression "field IS NOT NULL".
func IsNotNull(field string) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).IsNotNull(field)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).IsNotNull(field)
		case *BuilderDelete:
			return builder.(*BuilderDelete).IsNotNull(field)
		}

		return ""
	}
}

// Between is used to construct the expression "field BETWEEN lower AND upper".
func Between(field string, lower, upper interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).Between(field, lower, upper)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).Between(field, lower, upper)
		case *BuilderDelete:
			return builder.(*BuilderDelete).Between(field, lower, upper)
		}

		return ""
	}
}

// NotBetween is used to construct the expression "field NOT BETWEEN lower AND upper".
func NotBetween(field string, lower, upper interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).NotBetween(field, lower, upper)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).NotBetween(field, lower, upper)
		case *BuilderDelete:
			return builder.(*BuilderDelete).NotBetween(field, lower, upper)
		}

		return ""
	}
}

// Or is used to construct the expression OR logic like "expr1 OR expr2 OR expr3".
func Or(orExpr ...string) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).Or(orExpr...)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).Or(orExpr...)
		case *BuilderDelete:
			return builder.(*BuilderDelete).Or(orExpr...)
		}

		return ""
	}
}

// And is used to construct the expression AND logic like "expr1 AND expr2 AND expr3".
func And(andExpr ...string) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).And(andExpr...)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).And(andExpr...)
		case *BuilderDelete:
			return builder.(*BuilderDelete).And(andExpr...)
		}

		return ""
	}
}

// Not is used to construct the expression "NOT expr".
func Not(notExpr string) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).Not(notExpr)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).Not(notExpr)
		case *BuilderDelete:
			return builder.(*BuilderDelete).Not(notExpr)
		}

		return ""
	}
}

// Exists is used to construct the expression "EXISTS (subquery)".
func Exists(subquery interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).Exists(subquery)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).Exists(subquery)
		case *BuilderDelete:
			return builder.(*BuilderDelete).Exists(subquery)
		}

		return ""
	}
}

// NotExists is used to construct the expression "NOT EXISTS (subquery)".
func NotExists(subquery interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).NotExists(subquery)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).NotExists(subquery)
		case *BuilderDelete:
			return builder.(*BuilderDelete).NotExists(subquery)
		}

		return ""
	}
}

// Any is used to construct the expression "field op ANY (value...)".
func Any(field, op string, values ...interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).Any(field, op, values...)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).Any(field, op, values...)
		case *BuilderDelete:
			return builder.(*BuilderDelete).Any(field, op, values...)
		}

		return ""
	}
}

// All is used to construct the expression "field op ALL (value...)".
func All(field, op string, values ...interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).All(field, op, values...)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).All(field, op, values...)
		case *BuilderDelete:
			return builder.(*BuilderDelete).All(field, op, values...)
		}

		return ""
	}
}

// Some is used to construct the expression "field op SOME (value...)".
func Some(field, op string, values ...interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).Some(field, op, values...)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).Some(field, op, values...)
		case *BuilderDelete:
			return builder.(*BuilderDelete).Some(field, op, values...)
		}

		return ""
	}
}

// IsDistinctFrom is used to construct the expression "field IS DISTINCT FROM value".
//
// When the database system does not support the IS DISTINCT FROM operator,
// the NotILike method will return "NOT field <=> value" for MySQL or a
// "CASE ... WHEN ... ELSE ... END" expression to simulate the behavior of
// the IS DISTINCT FROM operator.
func IsDistinctFrom(field string, value interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).IsDistinctFrom(field, value)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).IsDistinctFrom(field, value)
		case *BuilderDelete:
			return builder.(*BuilderDelete).IsDistinctFrom(field, value)
		}

		return ""
	}
}

// IsNotDistinctFrom is used to construct the expression "field IS NOT DISTINCT FROM value".
//
// When the database system does not support the IS NOT DISTINCT FROM operator,
// the NotILike method will return "field <=> value" for MySQL or a
// "CASE ... WHEN ... ELSE ... END" expression to simulate the behavior of
// the IS NOT DISTINCT FROM operator.
func IsNotDistinctFrom(field string, value interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).IsNotDistinctFrom(field, value)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).IsNotDistinctFrom(field, value)
		case *BuilderDelete:
			return builder.(*BuilderDelete).IsNotDistinctFrom(field, value)
		}

		return ""
	}
}

// Var returns a placeholder for value.
func Var(value interface{}) ConditionFunc {
	return func(builder Builder) string {
		switch builder.(type) {
		case *BuilderSelect:
			return builder.(*BuilderSelect).Var(value)
		case *BuilderUpdate:
			return builder.(*BuilderUpdate).Var(value)
		case *BuilderDelete:
			return builder.(*BuilderDelete).Var(value)
		}

		return ""
	}
}
