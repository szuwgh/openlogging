%{
package temql
import "github.com/sophon-lab/temsearch/pkg/temql/labels"
import "fmt"
%}


%union {
    item Item
    node Node
    matchers  []*labels.Matcher
    matcher   *labels.Matcher
}

%token <item>
LEFT_PAREN
RIGHT_PAREN
LEFT_BRACE
RIGHT_BRACE    
LAND
LOR
EQL
IDENTIFIER
STRING
METRIC_IDENTIFIER
COMMA
ERROR
EOF

%type <item> match_op

%type <node>  expr term_expr term_identifier label_matchers vector_selector

%type <matchers> label_match_list

%type <matcher> label_matcher

%start start

%left LOR

%left LAND

%%

start           :
                expr
                    { yylex.(*parser).generatedParserResult = $1 }
                ;

expr            :
                vector_selector
                ;


match_op        : EQL  ;


term_identifier :  LEFT_PAREN term_expr RIGHT_PAREN
                        {
                           $$ = $2
                        }
                        |IDENTIFIER
                        {
                           $$ = yylex.(*parser).newTermExpr($1);   
                        }
                        ;

term_expr: IDENTIFIER
        {
           $$ = yylex.(*parser).newTermExpr($1);   
        }
        | LEFT_PAREN term_expr LAND term_expr RIGHT_PAREN
        {
           $$ = yylex.(*parser).newBinaryExpr($2, $3, $4)
        }
        | LEFT_PAREN term_expr LOR term_expr RIGHT_PAREN
        {
           $$ = yylex.(*parser).newBinaryExpr($2, $3, $4)
        }
        | term_expr LAND term_expr
        {
           $$ = yylex.(*parser).newBinaryExpr($1, $2, $3)
        }
        | term_expr LOR term_expr
        {
           $$ = yylex.(*parser).newBinaryExpr($1, $2, $3)
        }
        ;


label_matchers  : LEFT_BRACE label_match_list RIGHT_BRACE
                        {
                                $$ = &VectorSelector{
                                        LabelMatchers: $2,
                                }
                        }
                    ;

label_match_list: label_match_list COMMA label_matcher
                        {
                        if $1 != nil{
                                $$ = append($1, $3)
                        } else {
                                $$ = $1
                        }
                        }
                | label_matcher
                        { $$ = []*labels.Matcher{$1}}
                | label_match_list error
                        { yylex.(*parser).unexpected("label matching", "\",\" or \"}\""); $$ = $1 }
                ;

label_matcher   : IDENTIFIER match_op STRING
                        { $$ = yylex.(*parser).newLabelMatcher($1, $2, $3);  }
                | IDENTIFIER match_op error
                        { yylex.(*parser).unexpected("label matching", "string"); $$ = nil}
                | IDENTIFIER error
                        { yylex.(*parser).unexpected("label matching", "label matching operator"); $$ = nil }
                | error
                        { yylex.(*parser).unexpected("label matching", "identifier or \"}\""); $$ = nil}
                ;

vector_selector: term_identifier label_matchers
                {
                        vs := $2.(*VectorSelector)
                        vs.Expr = $1
                        $$ = vs
                }
                | term_identifier
                {
                     fmt.Println("parse term_identifier")
                     vs := &VectorSelector{
                                Expr: $1,
                                LabelMatchers: []*labels.Matcher{},
                        }
                     $$ = vs  
                }
                | label_matchers
                {
                        $$ = $1.(*VectorSelector)
                }
                |
                {
                         fmt.Println("parse nil")
                }
                ;

%%