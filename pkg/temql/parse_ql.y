%{
package temql
import (
        "github.com/szuwgh/temsearch/pkg/lib/prompb"
        "fmt"
        "github.com/szuwgh/temsearch/pkg/lib/prometheus/labels"
)
%}


%union {
    item Item
    node Node
    matchers  []*prompb.LabelMatcher
    matcher   *prompb.LabelMatcher
    label     labels.Label
    labels    labels.Labels
}

%token <item>
START_METRIC
START_EXPRESSION
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

%type <labels> label_set label_set_list metric
%type <label> label_set_item

%type <node>  expr term_expr term_identifier label_matchers vector_selector

%type <matchers> label_match_list

%type <matcher> label_matcher

%start start

%left LOR

%left LAND

%%

start           :
                START_METRIC metric
                    { yylex.(*parser).generatedParserResult = $2 }
                | START_EXPRESSION expr
                    { yylex.(*parser).generatedParserResult = $2 }
                ;

expr            :
                  vector_selector
                ;


match_op        : EQL  ;


metric          : 
                label_set
                        {$$ = $1}
                ;


label_set       : LEFT_BRACE label_set_list RIGHT_BRACE
                        { $$ = labels.New($2...) }
                | LEFT_BRACE label_set_list COMMA RIGHT_BRACE
                        { $$ = labels.New($2...) }
                | LEFT_BRACE RIGHT_BRACE
                        { $$ = labels.New() }
                | /* empty */
                        { $$ = labels.New() }
                ;

label_set_list  : label_set_list COMMA label_set_item
                        { $$ = append($1, $3) }
                | label_set_item
                        { $$ = []labels.Label{$1} }
                | label_set_list error
                        { yylex.(*parser).unexpected("label set", "\",\" or \"}\"", ); $$ = $1 }

                ;

label_set_item  : IDENTIFIER EQL STRING
                        { $$ = labels.Label{Name: $1.Val, Value: yylex.(*parser).unquoteString($3.Val) } }
                | IDENTIFIER EQL error
                        { yylex.(*parser).unexpected("label set", "string"); $$ = labels.Label{}}
                | IDENTIFIER error
                        { yylex.(*parser).unexpected("label set", "\"=\""); $$ = labels.Label{}}
                | error
                        { yylex.(*parser).unexpected("label set", "identifier or \"}\""); $$ = labels.Label{} }
                ;



term_identifier :  LEFT_PAREN term_expr RIGHT_PAREN
                        {
                           $$ = $2
                        }
                        | IDENTIFIER
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
                        { $$ = []*prompb.LabelMatcher{$1}}
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
                        vs.Expr = $1.(Expr)
                        $$ = vs
                }
                | term_identifier
                {
                     vs := &VectorSelector{
                                Expr: $1,
                                LabelMatchers: []*prompb.LabelMatcher{},
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