package cleansing_functions

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/artisoft-io/jetstore/jets/jetrules/rdf"
)

type CleansingFunctionContext struct {
	reMap                   map[string]*regexp.Regexp
	argdMap                 map[string]float64
	parsedFunctionArguments map[string]interface{}
	inputColumns            map[string]int
}

func NewCleansingFunctionContext(inputColumns map[string]int) *CleansingFunctionContext {
	return &CleansingFunctionContext{
		reMap:                   make(map[string]*regexp.Regexp),
		argdMap:                 make(map[string]float64),
		parsedFunctionArguments: make(map[string]interface{}),
		inputColumns:            inputColumns,
	}
}
func (ctx *CleansingFunctionContext) With(inputColumns map[string]int) *CleansingFunctionContext {
	return &CleansingFunctionContext{
		reMap:                   ctx.reMap,
		argdMap:                 ctx.argdMap,
		parsedFunctionArguments: ctx.parsedFunctionArguments,
		inputColumns:            inputColumns,
	}
}

// inputColumnName can be null
func (ctx *CleansingFunctionContext) ApplyCleasingFunction(functionName *string, argument *string, inputValue *string,
	inputPos int, inputRow *[]interface{}) (obj interface{}, errMsg string) {
	var err error
	var sz int
	switch *functionName {

	case "trim":
		vv := strings.TrimSpace(*inputValue)
		if len(vv) == 0 {
			obj = nil
		} else {
			obj = vv
		}

	case "validate_date":
		_, err2 := rdf.ParseDate(*inputValue)
		if err2 == nil {
			obj = *inputValue
		} else {
			errMsg = err2.Error()
		}

	case "to_upper":
		obj = strings.ToUpper(*inputValue)

	case "to_zip5":
		// Remove non digits characters
		inVal := filterDigits(*inputValue)
		sz = len(inVal)
		switch {
		case sz == 0:
			obj = nil
		case sz < 5:
			var v int
			v, err = strconv.Atoi(inVal)
			if err == nil {
				obj = fmt.Sprintf("%05d", v)
				if obj == "00000" {
					obj = nil
				}
			} else {
				errMsg = err.Error()
			}
		case sz == 5:
			obj = inVal
			if obj == "00000" {
				obj = nil
			}
		case sz > 5 && sz < 9:
			var v int
			v, err = strconv.Atoi(inVal)
			if err == nil {
				obj = fmt.Sprintf("%09d", v)[:5]
				if obj == "00000" {
					obj = nil
				}
			} else {
				errMsg = err.Error()
			}
		case sz == 9:
			obj = inVal[:5]
			if obj == "00000" {
				obj = nil
			}
		default:
		}

	case "to_zipext4_from_zip9": // from a zip9 input
		// Remove non digits characters
		inVal := filterDigits(*inputValue)
		sz = len(inVal)
		switch {
		case sz == 0:
			obj = nil
		case sz > 5 && sz < 9:
			var v int
			v, err = strconv.Atoi(inVal)
			if err == nil {
				obj = fmt.Sprintf("%09d", v)[5:]
				if obj == "0000" {
					obj = nil
				}
			} else {
				errMsg = err.Error()
			}
		case sz == 9:
			obj = inVal[5:]
			if obj == "0000" {
				obj = nil
			}
		default:
		}

	case "to_zipext4": // from a zip ext4 input
		// Remove non digits characters
		inVal := filterDigits(*inputValue)
		sz = len(inVal)
		switch {
		case sz == 0:
			obj = nil
		case sz < 4:
			var v int
			v, err = strconv.Atoi(inVal)
			if err == nil {
				obj = fmt.Sprintf("%04d", v)
				if obj == "0000" {
					obj = nil
				}
			} else {
				errMsg = err.Error()
			}
		case sz == 4:
			obj = inVal
			if obj == "0000" {
				obj = nil
			}
		default:
		}

	case "format_phone": // Validate & format phone according to E.164
		// Output: +1 area_code exchange_code subscriber_nbr
		// area_code: 3 digits, 1st digit is not 0 or 1
		// exchange_code: 3 digits, 1st digit is not 0 or 1
		// subscriber_nbr: 4 digits
		// Optional function argument is fmt.Sprintf formatter, expecting 3 string arguments (area_code, exchange_code, subscriber_nbr)
		inVal := filterDigits(*inputValue)
		if len(inVal) < 10 {
			errMsg = "too few digits"
			return obj, errMsg
		}
		if inVal[0] == '0' {
			inVal = inVal[1:]
		}
		if inVal[0] == '1' {
			inVal = inVal[1:]
		}
		if len(inVal) < 10 {
			errMsg = "invalid sequence of digits"
			return obj, errMsg
		}
		areaCode := inVal[0:3]
		exchangeCode := inVal[3:6]
		subscriberNbr := inVal[6:10]
		if areaCode[0] == '0' || areaCode[0] == '1' {
			errMsg = "invalid area code"
			return obj, errMsg
		}
		if exchangeCode[0] == '0' || exchangeCode[0] == '1' {
			errMsg = "invalid exchange code"
			return obj, errMsg
		}
		if len(*argument) == 0 {
			*argument = "+1%s%s%s"
		}
		obj = fmt.Sprintf(*argument, areaCode, exchangeCode, subscriberNbr)

	case "reformat0":
		if argument != nil {
			// Remove non digits characters
			inVal := filterDigits(*inputValue)
			var v int
			if len(inVal) == 0 {
				obj = nil
			} else {
				v, err = strconv.Atoi(inVal)
				if err == nil {
					obj = fmt.Sprintf(*argument, v)
				} else {
					errMsg = err.Error()
				}
			}
		} else {
			// configuration error, bailing out
			log.Panicf("ERROR missing argument for function reformat0 for input column pos %d", inputPos)
		}

	case "overpunch_number":
		if argument != nil {
			// Get the number of decimal position
			var npos int
			npos, err = strconv.Atoi(*argument)
			if err == nil {
				vv, err := OverpunchNumber(*inputValue, npos)
				if err != nil {
					obj = nil
					errMsg = err.Error()
				} else {
					if len(vv) == 0 {
						obj = nil
					} else {
						obj = vv
					}
				}
			} else {
				obj = nil
				errMsg = err.Error()
			}
		} else {
			// configuration error, bailing out
			log.Panicf("ERROR missing argument for function overpunch_number for input column pos %d", inputPos)
		}

	case "apply_regex":
		if argument != nil {
			re, ok := ctx.reMap[*argument]
			if !ok {
				re, err = regexp.Compile(*argument)
				if err != nil {
					// configuration error, bailing out
					log.Panicf("ERROR regex argument does not compile: %s", *argument)
				}
				ctx.reMap[*argument] = re
			}
			vv := re.FindString(*inputValue)
			if len(vv) == 0 {
				obj = nil
			} else {
				obj = vv
			}
		} else {
			// configuration error, bailing out
			log.Panicf("ERROR missing argument for function apply_regex for input column pos %d", inputPos)
		}

	case "scale_units":
		if argument != nil {
			if *argument == "1" {
				vv := filterDouble(*inputValue)
				if len(vv) == 0 {
					obj = nil
				} else {
					obj = vv
				}
			} else {
				divisor, ok := ctx.argdMap[*argument]
				if !ok {
					divisor, err = strconv.ParseFloat(*argument, 64)
					if err != nil {
						// configuration error, bailing out
						log.Panicf("ERROR divisor argument to function scale_units is not a double: %s", *argument)
					}
					ctx.argdMap[*argument] = divisor
				}
				// Remove non digits characters
				inVal := filterDouble(*inputValue)
				var unit float64
				unit, err = strconv.ParseFloat(inVal, 64)
				if err == nil {
					obj = fmt.Sprintf("%f", math.Ceil(unit/divisor))
				} else {
					obj = nil
					errMsg = err.Error()
				}
			}
		} else {
			// configuration error, bailing out
			log.Panicf("ERROR missing argument for function scale_units for input column pos %d", inputPos)
		}

	case "parse_amount":
		// clean up the amount
		inVal := filterDouble(*inputValue)
		if len(inVal) > 0 {
			obj = inVal
			// argument is optional, assume divisor is 1 if absent
			if argument != nil && *argument != "1" {
				divisor, ok := ctx.argdMap[*argument]
				if !ok {
					divisor, err = strconv.ParseFloat(*argument, 64)
					if err != nil {
						// configuration error, bailing out
						log.Panicf("ERROR divisor argument to function scale_units is not a double: %s", *argument)
					}
					ctx.argdMap[*argument] = divisor
				}
				var amt float64
				amt, err = strconv.ParseFloat(inVal, 64)
				if err == nil {
					obj = fmt.Sprintf("%f", amt/divisor)
				} else {
					obj = nil
					errMsg = err.Error()
				}
			}
		}

	case "concat", "concat_with":
		// Cleansing function that concatenate inputRow columns w delimiter
		// Get the parsed argument
		arg, err := ParseConcatFunctionArgument(argument, *functionName, ctx.inputColumns, ctx.parsedFunctionArguments, inputRow)
		if err != nil {
			errMsg = err.Error()
		} else {
			var buf strings.Builder
			buf.WriteString(*inputValue)
			for i := range arg.ColumnPositions {
				// fmt.Println("=== concat value @pos:",arg.ColumnPositions[i])
				if (*inputRow)[arg.ColumnPositions[i]] != nil {
					if arg.Delimit != "" {
						buf.WriteString(arg.Delimit)
					}
					switch vv := (*inputRow)[arg.ColumnPositions[i]].(type) {
					case string:
						buf.WriteString(vv)
					case *sql.NullString:
						if vv.Valid {
							buf.WriteString(vv.String)
						}
					default:
						buf.WriteString(fmt.Sprint(vv))
					}

				}
			}
			vv := buf.String()
			if len(vv) == 0 {
				obj = nil
			} else {
				obj = vv
			}
		}

	case "find_and_replace":
		// Cleansing function that replace portion of the input column
		// Get the parsed argument
		arg, err := ParseFindReplaceFunctionArgument(argument, *functionName, ctx.parsedFunctionArguments)
		if err != nil {
			errMsg = err.Error()
		} else {
			vv := strings.ReplaceAll(*inputValue, arg.Find, arg.ReplaceWith)
			if len(vv) == 0 {
				obj = nil
			} else {
				obj = vv
			}
		}

	case "substring":
		// Cleansing function that takes a substring of input columns
		// Get the parsed argument
		arg, err := ParseSubStringFunctionArgument(argument, *functionName, ctx.parsedFunctionArguments)
		if err != nil {
			errMsg = err.Error()
		} else {
			end := arg.End
			if end < 0 {
				end = len(*inputValue) + end
			}
			if end > len(*inputValue) || end <= arg.Start {
				obj = nil
			} else {
				obj = (*inputValue)[arg.Start:end]
			}
		}

	case "split_on":
		if argument != nil {
			obj = SplitOn(inputValue, argument)
		} else {
			// configuration error, bailing out
			log.Panicf("ERROR missing argument for function split_on for input column pos %d", inputPos)
		}

	case "unique_split_on":
		if argument != nil {
			obj = UniqueSplitOn(inputValue, argument)
		} else {
			// configuration error, bailing out
			log.Panicf("ERROR missing argument for function split_on for input column pos %d", inputPos)
		}

	case "slice_input":
		if inputValue == nil {
			obj = nil
		}	else {
		if argument != nil {
			obj = SliceInput(*inputValue, *argument, ctx.parsedFunctionArguments)
		} else {
			// configuration error, bailing out
			log.Panicf("ERROR missing argument for function slice_input for input column pos %d", inputPos)
		}
		}

	default:
		log.Panicf("ERROR unknown mapping function: %s", *functionName)
	}

	return obj, errMsg
}

func SliceInput(inputValue, argument string, parsedFunctionArguments map[string]any) any {
	if inputValue == "" {
		return nil
	}
	sliceArg, err := ParseSliceInputFunctionArgument(argument, "slice_input", parsedFunctionArguments)
	if err != nil {
		log.Panicf("while parsing arguments for cleansing function parse_input: %v", err)
	}
	sliceValues := strings.Split(inputValue, sliceArg.Delimit)
	l := len(sliceValues)
	var values []string
	switch {
	case l == 0:
		return nil
	case sliceArg.From == nil && sliceArg.To == nil && sliceArg.Values == nil:
		return sliceValues
	case sliceArg.Values != nil:
		values = make([]string, 0, len(*sliceArg.Values))
		for _, i := range *sliceArg.Values {
			if i < l {
				values = append(values, sliceValues[i])
			}
		}
	case sliceArg.From != nil && sliceArg.To == nil:
		lenValues := l - *sliceArg.From
		values = make([]string, 0, lenValues)
		for i := range lenValues {
			index := *sliceArg.From + i
			if index < l {
				values = append(values, sliceValues[index])
			}
		}
	default:
		lenValues := *sliceArg.To - *sliceArg.From
		values = make([]string, 0, lenValues)
		for i := range lenValues {
			index := *sliceArg.From + i
			if index < l {
				values = append(values, sliceValues[index])
			}
		}
	}
	return values
}

type SliceInputFunctionArg struct {
	Delimit string
	Values  *[]int
	From    *int
	To      *int
}

func ParseSliceInputFunctionArgument(rawArg string, functionName string, cache map[string]any) (*SliceInputFunctionArg, error) {
	// rawArg is csv-encoded as: "delimit","from",":","to"
	// rawArg is csv-encoded as: "delimit","v1","v2","v3",...
	// delimit is text
	// Case "delimit","from",":","to":
	//	- when: "delimit","from",":" then take all elements starting at `from` (encoded as nil for To).
	//	- otherwise take input[from:to] (from is inclusive and to is exclusive)
	// Case "delimit","v1","v2","v3",...
	//	- when only delimit is provided, take all values (encoded as nil for values, from and to).
	//	- when "v1","v2","v3",..., take only the specified element (0-based) (encoded as values, nil for from and to).
	//	Note: when value is negative, it means len(input) - value (this applies to from and to as well)
	if rawArg == "" {
		return nil, fmt.Errorf("unexpected null argument to %s function", functionName)
	}
	// Check if we have it cached
	key := fmt.Sprintf("%s(%s)", functionName, rawArg)
	v := cache[key]
	if v != nil {
		fmt.Println("*** OK Got Cached value for", rawArg, ":", v)
		return v.(*SliceInputFunctionArg), nil
	}
	// Parsed the raw argument into SliceInputFunctionArg and put it in the cache
	rows, err := Parse(rawArg)
	if len(rows) == 0 || len(rows[0]) == 0 || err != nil {
		// It's not csv or config not valid
		return nil, fmt.Errorf("error: no-data: argument '%s' cannot be parsed as csv or is invalid: %v (%s function)", rawArg, err, functionName)
	}
	var results *SliceInputFunctionArg
	switch {
	case len(rows[0]) == 1:
		results = &SliceInputFunctionArg{
			Delimit: rows[0][0],
		}
	case len(rows[0]) == 2:
		v1, err := strconv.Atoi(strings.TrimSpace(rows[0][1]))
		if err != nil {
			return nil, fmt.Errorf("error: invalid argument '%s' expecting int value as second argument: %v (%s function)", rawArg, err, functionName)
		}
		results = &SliceInputFunctionArg{
			Delimit: rows[0][0],
			Values:  &[]int{v1},
		}
	case len(rows[0]) > 2 && rows[0][2] == ":":
		from, err := strconv.Atoi(strings.TrimSpace(rows[0][1]))
		if err != nil {
			return nil, fmt.Errorf("error: invalid argument '%s' expecting from (int) as second argument: %v (%s function)", rawArg, err, functionName)
		}
		switch len(rows[0]) {
		case 3:
			results = &SliceInputFunctionArg{
				Delimit: rows[0][0],
				From:    &from,
			}
		case 4:
			to, err := strconv.Atoi(strings.TrimSpace(rows[0][3]))
			if err != nil {
				return nil, fmt.Errorf("error: invalid argument '%s' expecting to (int) as forth argument: %v (%s function)", rawArg, err, functionName)
			}
			results = &SliceInputFunctionArg{
				Delimit: rows[0][0],
				From:    &from,
				To:      &to,
			}
		default:
			return nil, fmt.Errorf("error: invalid argument '%s' expecting \"from\",\":\",\"to\" construct (%s function)", rawArg, functionName)
		}
	default:
		values := make([]int, 0, len(rows[0])-1)
		for _, vstr := range rows[0][1:] {
			v, err := strconv.Atoi(strings.TrimSpace(vstr))
			if err != nil {
				return nil, fmt.Errorf("error: invalid argument '%s' expecting int value as argument: %v (%s function)", rawArg, err, functionName)
			}
			values = append(values, v)
		}
		results = &SliceInputFunctionArg{
			Delimit: rows[0][0],
			Values:  &values,
		}
	}
	cache[key] = results
	return results, nil
}
type Chartype rune

// Single character type for csv options
func (s *Chartype) String() string {
	return string(rune(*s))
}

func (s *Chartype) Set(value string) error {
	r := []rune(value)
	if len(r) > 1 || r[0] == '\n' {
		return errors.New("sep must be a single char not '\\n'")
	}
	*s = Chartype(r[0])
	return nil
}

func DetectDelimiter(buf []byte) (sep_flag Chartype, err error) {
	// auto detect the separator based on the first line
	nb := len(buf)
	if nb > 2048 {
		nb = 2048
	}
	txt := string(buf[0:nb])
	cn := strings.Count(txt, ",")
	pn := strings.Count(txt, "|")
	tn := strings.Count(txt, "\t")
	td := strings.Count(txt, "~")
	switch {
	case (cn > pn) && (cn > tn) && (cn > td):
		sep_flag = ','
	case (pn > cn) && (pn > tn) && (pn > td):
		sep_flag = '|'
	case (tn > cn) && (tn > pn) && (tn > td):
		sep_flag = '\t'
	case (td > cn) && (td > pn) && (td > tn):
		sep_flag = '~'
	default:
		return 0, fmt.Errorf("error: cannot determine the csv-delimit used in buf")
	}
	return
}

// Parse the csvBuf, if cannot determine the separator, will assume it's a single column
// and default to use the ','
func Parse(csvBuf string) ([][]string, error) {
	byteBuf := []byte(csvBuf)
	sepFlag, err := DetectDelimiter(byteBuf)
	if err != nil {
		// Cannot detect delimiter, assume it's a single column
		sepFlag = ','
	}
	r := csv.NewReader(bytes.NewReader(byteBuf))
	r.Comma = rune(sepFlag)
	results := make([][]string, 0)
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("while parsing csv row: %v", err)
		}
		results = append(results, row)
	}
	return results, nil
}

func SplitOn(inputValue, argument *string) interface{} {
	if inputValue == nil || argument == nil || *inputValue == "" {
		return nil
	}
	return strings.Split(*inputValue, *argument)
}

func UniqueSplitOn(inputValue, argument *string) interface{} {
	if inputValue == nil || argument == nil || *inputValue == "" {
		return nil
	}
	vv := strings.Split(*inputValue, *argument)
	// vv may contains duplicated value, to make each value unique we append -%d to the
	// value, where %d is the value of a counter such that:
	//   if *inputValue is "value1,value2,value1,value3"
	//   then the parsed values will be:
	//     value1-0
	//     value1-1
	//     value2-0
	//     value3-0
	// Group the common values
	cm := make(map[string]*[]string)
	for _, v := range vv {
		cv := cm[v]
		if cv == nil {
			cv = &[]string{}
			cm[v] = cv
		}
		*cv = append(*cv, v)
	}
	// reuse vv
	vv = vv[:0]
	for _, cv := range cm {
		for i := range (*cv) {
			vv = append(vv, fmt.Sprintf("%s-%d", (*cv)[i], i))
		}
	}
	return vv
}
