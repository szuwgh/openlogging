%{
package parse

%}


%union {
    item Item
    node Node
}

%token <item>
LEFT_PAREN
RIGHT_PAREN
LEFT_BRACE
RIGHT_BRACE    
ASSIGN
LAND
LOR
EQL
IDENTIFIER
STRING
METRIC_IDENTIFIER
COMMA


%type <item> match_op metric_identifier 

%type <node>  expr label_matchers vector_selector

%type <matchers> label_match_list

%type <matcher> label_matcher

%start start


%%



start           :
                expr
                    { yylex.(*parser).generatedParserResult = $1 }
                ;

expr            :
                vector_selector
                ;


match_op        : EQL  ;

metric_identifier : METRIC_IDENTIFIER   ;

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

vector_selector: metric_identifier label_matchers
                        {
                        vs := $2.(*VectorSelector)
                        vs.Name = $1.Val
                        yylex.(*parser).assembleVectorSelector(vs)
                        $$ = vs
                        }
                    ;

%%