package csvquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type InfixNotationTestSuite struct {
	suite.Suite
	notation          *InfixNotation
	infixTokens       []string
	wantPostfixTokens []string
}

func (n *InfixNotationTestSuite) SetupTest() {
	n.notation = NewInfixNotation()
	n.infixTokens = []string{"(", "COND0", "AND", "COND1", "AND", "COND2", ")", "OR", "(", "COND3", "OR", "COND4", "AND", "COND5", ")"}
	n.wantPostfixTokens = []string{"COND0", "COND1", "AND", "COND2", "AND", "COND3", "COND4", "COND5", "AND", "OR", "OR"}

	for _, token := range n.infixTokens {
		n.notation.AddToken(token)
	}
}

func (n *InfixNotationTestSuite) TestInfixNotation_AddToken() {
	assert.Equal(n.T(), n.infixTokens, n.notation.tokens)
	assert.Equal(n.T(), len(n.infixTokens), n.notation.Size())
	assert.Equal(n.T(), n.wantPostfixTokens, n.notation.ToPostfix())
}

func TestInfixNotationTestSuite(t *testing.T) {
	suite.Run(t, new(InfixNotationTestSuite))
}
