package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func main() {
	dir := "." // Change this to the directory you want to scan
	files, err := listGoFiles(dir)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	for _, file := range files {
		println(file)
		content, err := readFile(file)
		if err != nil {
			fmt.Printf("Error reading file: %v\n", err)
			return
		}

		codeBlocks := findCodeBlocks(content)
		testBlocks, err := GenerateTestBlocks(file, codeBlocks...)
		if err != nil {
			fmt.Printf("failed generating test blocks: %v\n", err)
			return
		}
		for _, block := range testBlocks {
			fmt.Println("Generated test block:")
			fmt.Println(block)
			fmt.Println()
		}
	}
}

// listGoFiles recursively lists all .go files in the specified directory.
func listGoFiles(dir string) ([]string, error) {
	var goFiles []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(path, "vendor") {
			return nil
		}

		if !info.IsDir() && filepath.Ext(path) == ".go" {
			goFiles = append(goFiles, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return goFiles, nil
}

func readFile(filename string) (string, error) {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// findCodeBlocks finds all code blocks in the content.
func findCodeBlocks(content string) []string {
	var codeBlocks []string
	//startReg := regexp.MustCompile(`(QueryTable|GetQuery)\[\w+]\(\w+\)?.`)

	// Regular expression to find multiline statements with method chaining
	re := regexp.MustCompile(`(?m)^.*=.*(QueryTable|GetQuery)\[\w+]\(\w+\)[\s\S]*?(\.Run\(ctx\s*,\s*nil\)|\.RunSingle\(ctx\s*,\s*nil\)|\.SelectQuery\[\w+,\s\w+]\(\w+,\s\w+, \w+\))`)
	matches := re.FindAllString(content, -1)
	for _, match := range matches {
		if strings.Contains(match, "func") {
			continue
		}
		codeBlocks = append(codeBlocks, match)
	}
	return codeBlocks
}

//((QueryTable|GetQuery)\[\w+]\(\w+\)((.|\n)+)(.Build\(\)|.Run\(.*\)))

func GenerateTestBlocks(file string, codeBlocks ...string) ([]string, error) {
	var testBlocks []string

	for i, block := range codeBlocks {
		name, err := ExtractGenericTypeFromCode(block)
		if err != nil {
			continue
		}
		b := fmt.Sprintf(testBlock, name, i+1, name, "", block)

		vars, err := FindUndeclaredVariables(b)
		if err != nil {
			continue
		}
		init := []string{}
		for _, v := range vars {
			init = append(init, fmt.Sprintf("%s interface{}", v))
		}

		b = fmt.Sprintf(testBlock, name, i+1, name, strings.Join(init, "\n"), block)
		unused, _ := FindUnusedVariables(b)
		println(strings.Join(unused, ""))
		testBlocks = append(testBlocks, b)
	}

	return testBlocks, nil
}
func ExtractGenericTypeFromCode(code string) (string, error) {
	re := regexp.MustCompile(`GetQuery\[(\w+)\]`)
	matches := re.FindStringSubmatch(code)
	if len(matches) > 1 {
		return matches[1], nil
	}
	return "", fmt.Errorf("no generic type found")
}

// FindUndeclaredVariables analyzes Go code and returns a list of variables that are used but not declared.
func FindUndeclaredVariables(code string) ([]string, error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, "", code, parser.AllErrors)
	if err != nil {
		return nil, err
	}

	declaredVars := make(map[string]bool)
	usedVars := make(map[string]bool)
	allVars, err := FindAllVariables(code)
	if err != nil {
		return nil, err
	}
	for _, f := range allVars {
		usedVars[f] = true
	}
	// Inspect the AST
	ast.Inspect(node, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.GenDecl:
			if n.Tok == token.VAR || n.Tok == token.CONST {
				for _, spec := range n.Specs {
					if v, ok := spec.(*ast.ValueSpec); ok {
						for _, name := range v.Names {
							declaredVars[name.Name] = true
						}
					}
				}
			}

		case *ast.AssignStmt:
			// Mark variables as declared in assignment statements
			for _, lhs := range n.Lhs {
				if id, ok := lhs.(*ast.Ident); ok {
					declaredVars[id.Name] = true
				}
			}
		case *ast.Ident:
			// Ensure it's a variable by checking if it has not been declared
			//if _, declared := declaredVars[n.Name]; !declared && isVariable(n) {
			//	usedVars[n.Name] = true
			//}
		}
		return true
	})

	// Find variables that are used but not declared
	var undeclaredVars []string
	for name := range usedVars {
		if _, declared := declaredVars[name]; !declared {
			undeclaredVars = append(undeclaredVars, name)
		}
	}

	return undeclaredVars, nil
}

// FindAllVariables analyzes Go code and returns a list of all variable names.
func FindAllVariables(code string) ([]string, error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, "", code, parser.AllErrors)
	if err != nil {
		return nil, err
	}

	variables := make(map[string]bool)

	// Inspect the AST
	ast.Inspect(node, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.GenDecl:
			if n.Tok == token.VAR || n.Tok == token.CONST {
				for _, spec := range n.Specs {
					if v, ok := spec.(*ast.ValueSpec); ok {
						for _, name := range v.Names {
							variables[name.Name] = true
						}
					}
				}
			}
		case *ast.CallExpr:
			for _, param := range n.Args {
				if id, ok := param.(*ast.Ident); ok {
					variables[id.Name] = true
				}
			}
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				if id, ok := lhs.(*ast.Ident); ok {
					variables[id.Name] = true
				}
			}
		case *ast.FuncDecl:
			// Check function parameters
			for _, param := range n.Type.Params.List {
				for _, name := range param.Names {
					variables[name.Name] = true
				}
			}
			// Check function return values
			if n.Type.Results == nil {
				return true
			}
			for _, result := range n.Type.Results.List {
				for _, name := range result.Names {
					variables[name.Name] = true
				}
			}
		}
		return true
	})

	// Convert map keys to slice
	var variableNames []string
	for name := range variables {
		if name == "nil" || name == "t" {
			continue
		}
		variableNames = append(variableNames, name)
	}

	return variableNames, nil
}

var testBlock = `
package main

func TestGeneratedBlock%s_%d(t *testing.T) {
	var err error
	ctx := context.Background() // Adjust as needed	// Here, you can set up any required state or mocks

	dao := db.NewMockDAO()
	ctx, err = db.AddTable[%s](ctx, dao, "test", "sql")
	if err != nil {
		t.Fatalf("failed adding table: %%v", err)
	}

	// Arrange
	var (
	%s
	)
	// Here, you can set up any required state or mocks

	// Act
%s

	// Assert
	// Here, you should verify the expected outcomes using assertions
}


`

// FindUnusedVariables analyzes Go code and returns a list of unused variables.
func FindUnusedVariables(code string) ([]string, error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, "", code, parser.AllErrors)
	if err != nil {
		return nil, err
	}

	variables := make(map[string]bool) // Track declared variables
	usedVars := make(map[string]bool)  // Track used variables

	// Inspect the AST to find variable declarations and usage
	ast.Inspect(node, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.GenDecl:
			if n.Tok == token.VAR || n.Tok == token.CONST {
				for _, spec := range n.Specs {
					if v, ok := spec.(*ast.ValueSpec); ok {
						for _, name := range v.Names {
							variables[name.Name] = true
						}
					}
				}
			}
		case *ast.AssignStmt:
			for _, lhs := range n.Lhs {
				if id, ok := lhs.(*ast.Ident); ok {
					variables[id.Name] = true
				}
			}
		case *ast.Ident:
			if _, ok := variables[n.Name]; ok {
				usedVars[n.Name] = true
			}
		}
		return true
	})

	// Determine unused variables
	var unusedVars []string
	for name := range variables {
		if _, used := usedVars[name]; !used {
			unusedVars = append(unusedVars, name)
		}
	}

	return unusedVars, nil
}
