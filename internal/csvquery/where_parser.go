package csvquery

import (
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"go.uber.org/zap"

	"github.com/phpCoder88/csv-searcher/internal/structs"
)

type WhereParser struct {
	where        string
	cursor       int
	columns      map[Column]int
	bracketStack structs.StringStack
	tokenStack   WhereTokenStack
	tokens       *InfixNotation
	condMap      ConditionMap
	logger       *zap.Logger
}

func NewWhereParser(where string, logger *zap.Logger) *WhereParser {
	return &WhereParser{
		where:   where,
		condMap: make(ConditionMap),
		tokens:  NewInfixNotation(),
		columns: map[Column]int{},
		logger:  logger,
	}
}

func (p *WhereParser) Parse() (map[Column]int, *structs.Tree, error) {
	err := p.processWhereStmt()
	if err != nil {
		return nil, nil, err
	}

	postfix := p.tokens.ToPostfix()

	return p.columns, p.parseToTree(postfix), nil
}

func (p *WhereParser) processWhereStmt() error {
	for p.cursor < len(p.where) {
		runeValue, _ := utf8.DecodeRuneInString(p.where[p.cursor:])
		var err error

		if p.tokenStack.IsEmpty() || p.tokenStack.IsTopEqual(OpenBracketToken) {
			// 1. (
			// 2. Condition
			err = p.nextAfterOpenBracketOrEmptyToken(runeValue)
		} else if p.tokenStack.IsTopEqual(CloseBracketToken) || p.tokenStack.IsTopEqual(CondToken) {
			// 1. )
			// 2. Binary Operator
			// 3. End of line
			err = p.nextAfterCloseBracketOrCondToken(runeValue)
		} else if p.tokenStack.IsTopEqual(BinaryOpToken) {
			// 1. (
			// 2. Binary Operator
			err = p.nextAfterBinaryOpToken(runeValue)
		}

		if err != nil {
			return err
		}

		p.skipSpace()
	}

	if p.bracketStack.IsNotEmpty() {
		p.logger.Error(ErrIncorrectBracketPosition.Error())
		return ErrIncorrectBracketPosition
	}

	if p.tokens.Size() == 0 {
		p.logger.Error(ErrIncorrectQuery.Error())
		return ErrIncorrectQuery
	}

	return nil
}

func (p *WhereParser) findCondition() (*Condition, error) {
	column, err := p.extractConditionColumn()
	if err != nil {
		return nil, err
	}

	p.columns[column] = 0
	p.skipSpace()

	op, err := p.extractConditionOperator()
	if err != nil {
		return nil, err
	}

	p.skipSpace()

	value, valueType, err := p.extractConditionValue()
	if err != nil {
		return nil, err
	}

	p.skipSpace()

	return &Condition{
		Column:    column,
		Op:        op,
		Value:     value,
		ValueType: valueType,
	}, nil
}

func (p *WhereParser) extractConditionColumn() (Column, error) {
	var colEndPos int
	for i, char := range p.where[p.cursor:] {
		if char == '<' || char == '>' || char == '=' || char == ' ' {
			colEndPos = i
			break
		}
	}

	if colEndPos == 0 {
		p.logger.Error(ErrIncorrectQuery.Error())
		return "", ErrIncorrectQuery
	}

	column := p.where[p.cursor : p.cursor+colEndPos]
	p.cursor += len(column)

	return Column(column), nil
}

func (p *WhereParser) extractConditionOperator() (Operation, error) {
	var op string
	for _, opItem := range Operations {
		posOp := p.where[p.cursor : p.cursor+len(opItem)]
		if strings.EqualFold(posOp, opItem) && len(op) < len(posOp) {
			op = posOp
		}
	}

	if op == "" {
		p.logger.Error(ErrIncorrectQuery.Error())
		return "", ErrIncorrectQuery
	}
	p.cursor += len(op)

	return Operation(op), nil
}

func (p *WhereParser) extractConditionValue() (value interface{}, valueType ValueType, err error) {
	if strings.HasPrefix(p.where[p.cursor:], "'") || strings.HasPrefix(p.where[p.cursor:], "\"") {
		// поиск строки
		value, err = p.extractStringConditionValue()
		if err != nil {
			return nil, 0, err
		}

		valueType = TypeString
	} else {
		// поиск числа и преобразование строки в число
		value, err = p.extractNumberConditionValue()
		if err != nil {
			return nil, 0, err
		}

		valueType = TypeNumber
	}

	return value, valueType, nil
}

func (p *WhereParser) extractStringConditionValue() (string, error) {
	openStringChar := p.where[p.cursor : p.cursor+1]
	var endStrPos int

	strCursor := p.cursor
	for strCursor < len(p.where) {
		endStrPos = strings.Index(p.where[strCursor+1:], openStringChar)
		if endStrPos == -1 {
			p.logger.Error(ErrIncorrectQuery.Error())
			return "", ErrIncorrectQuery
		}

		prevSymbol := p.where[strCursor+endStrPos : strCursor+endStrPos+1]
		if prevSymbol != "\\" {
			break
		}
		strCursor += endStrPos + 1
	}

	value := p.where[p.cursor+1 : strCursor+endStrPos+1]

	value = strings.Replace(value, `\"`, `"`, -1)
	value = strings.Replace(value, `\"`, `"`, -1)

	p.cursor = strCursor + endStrPos + 2

	return value, nil
}

func (p *WhereParser) extractNumberConditionValue() (float64, error) {
	spacePos := strings.Index(p.where[p.cursor:], " ")
	parenthesisPos := strings.Index(p.where[p.cursor:], ")")

	var valueStr string
	if spacePos == -1 && parenthesisPos == -1 {
		valueStr = p.where[p.cursor:]
	} else if spacePos != -1 && parenthesisPos != -1 {
		valueStr = p.where[p.cursor : p.cursor+int(math.Min(float64(spacePos), float64(parenthesisPos)))]
	} else if spacePos != -1 {
		valueStr = p.where[p.cursor : p.cursor+spacePos]
	} else {
		valueStr = p.where[p.cursor : p.cursor+parenthesisPos]
	}

	p.cursor += len(valueStr)

	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		p.logger.Error(ErrIncorrectQuery.Error())
		return 0, ErrIncorrectQuery
	}

	return value, nil
}

func (p *WhereParser) findBinaryOperator() (string, error) {
	opEndPos := strings.Index(p.where[p.cursor:], " ")
	if opEndPos == -1 {
		p.logger.Error(ErrIncorrectQuery.Error())
		return "", ErrIncorrectQuery
	}

	op := strings.ToUpper(p.where[p.cursor : p.cursor+opEndPos])
	if op != string(AndKeyword) && op != string(OrKeyword) {
		p.logger.Error(ErrIncorrectQuery.Error())
		return "", ErrIncorrectQuery
	}

	p.cursor += len(op)

	return op, nil
}

func (p *WhereParser) skipSpace() {
	for _, char := range p.where[p.cursor:] {
		if char != ' ' {
			break
		}
		p.cursor++
	}
}

func (p *WhereParser) addOpenBracketToken() {
	p.tokenStack.Push(OpenBracketToken)
	p.bracketStack.Push(string(OpenBracketToken))
	p.tokens.AddToken(string(OpenBracketToken))
	p.cursor++
}

func (p *WhereParser) nextAfterOpenBracketOrEmptyToken(currentRune rune) error {
	var err error
	if currentRune == '(' {
		p.addOpenBracketToken()
	} else {
		err = p.processCondition()
	}

	return err
}

func (p *WhereParser) nextAfterCloseBracketOrCondToken(currentRune rune) error {
	var err error

	if currentRune == ')' {
		err = p.processCloseBracket()
	} else {
		err = p.processBinaryOperator()
	}

	return err
}

func (p *WhereParser) nextAfterBinaryOpToken(currentRune rune) error {
	var err error

	if currentRune == '(' {
		p.addOpenBracketToken()
	} else {
		err = p.processCondition()
	}

	return err
}

func (p *WhereParser) processCloseBracket() error {
	if !p.bracketStack.IsTopEqual("(") {
		p.logger.Error(ErrIncorrectBracketPosition.Error())
		return ErrIncorrectBracketPosition
	}

	_, _ = p.bracketStack.Pop()
	p.tokenStack.Push(CloseBracketToken)
	p.tokens.AddToken(string(CloseBracketToken))
	p.cursor++

	return nil
}

func (p *WhereParser) processCondition() error {
	cond, err := p.findCondition()
	if err != nil {
		return err
	}

	p.tokenStack.Push(CondToken)
	expKey := p.condMap.Add(cond)
	p.tokens.AddToken(expKey)

	return nil
}

func (p *WhereParser) processBinaryOperator() error {
	op, err := p.findBinaryOperator()
	if err != nil {
		return err
	}

	p.tokenStack.Push(BinaryOpToken)
	p.tokens.AddToken(op)

	return nil
}

func (p *WhereParser) parseToTree(postfix []string) *structs.Tree {
	stack := structs.TreeStack{}
	for _, item := range postfix {
		if !IsOperator(item) {
			node := structs.NewTree(p.condMap[item], nil, nil)
			stack.Push(node)
		} else {
			right, _ := stack.Pop()
			left, _ := stack.Pop()
			newTree := structs.NewTree(item, left, right)

			stack.Push(newTree)
		}
	}

	whereTree, _ := stack.Pop()

	return whereTree
}
