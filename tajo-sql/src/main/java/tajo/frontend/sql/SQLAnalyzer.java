package tajo.frontend.sql;

import com.google.common.base.Preconditions;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import tajo.algebra.*;
import tajo.algebra.LiteralExpr.LiteralType;
import tajo.engine.planner.JoinType;

public class SQLAnalyzer {
  public RelationalOp parse(String sql) {
    ParsingContext context = new ParsingContext(sql);
    CommonTree tree = parseSQL(sql);
    return transform(context, tree);
  }

  private static class ParsingContext {
    private String rawQuery;
    public ParsingContext(String sql) {
      this.rawQuery = sql;
    }
  }

  private static CommonTree parseSQL(final String query) {
    ANTLRStringStream input = new ANTLRStringStream(query);
    SQLLexer lexer = new SQLLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokens);

    CommonTree ast;
    try {
      ast = ((CommonTree) parser.statement().getTree());
    } catch (RecognitionException e) {
      throw new SQLParseError(e.getMessage());
    }

    return ast;
  }

  private RelationalOp transform(ParsingContext context, CommonTree ast) {

    switch (ast.getType()) {
      case SQLParser.SELECT:
        return parseSelectStatement(context, ast);
      case SQLParser.UNION:

      case SQLParser.EXCEPT:

      case SQLParser.INTERSECT:

      case SQLParser.INSERT:

      case SQLParser.CREATE_INDEX:

      case SQLParser.CREATE_TABLE:

      case SQLParser.DROP_TABLE:

      default:
        return null;
    }
  }

  private RelationalOp parseSelectStatement(ParsingContext context,
                                          final CommonTree ast) {
    RelationalOp current = null;
    CommonTree node;
    for (int cur = 0; cur < ast.getChildCount(); cur++) {
      node = (CommonTree) ast.getChild(cur);

      switch (node.getType()) {
        case SQLParser.FROM:
          current = parseFromClause(context, node);
          break;

        case SQLParser.SET_QUALIFIER:
          break;

        case SQLParser.SEL_LIST:
          ProjectionOp proj = parseSelectList(node);
          proj.setSubOp(current);
          current = proj;
          break;

        case SQLParser.WHERE:
          break;

        case SQLParser.GROUP_BY:
          break;

        case SQLParser.HAVING:
          break;

        case SQLParser.ORDER_BY:

          break;

        case SQLParser.LIMIT:

          break;

        default:

      }
    }

    return current;
  }

  /**
   * This method parses the select list of a query statement.
   * <pre>
   * EBNF:
   *
   * selectList
   * : MULTIPLY -> ^(SEL_LIST ALL)
   * | derivedColumn (COMMA derivedColumn)* -> ^(SEL_LIST derivedColumn+)
   * ;
   *
   * derivedColumn
   * : bool_expr asClause? -> ^(COLUMN bool_expr asClause?)
   * ;
   *
   * @param ast
   */
  private ProjectionOp parseSelectList(final CommonTree ast) {
    ProjectionOp projectionOp = new ProjectionOp();
    if (ast.getChild(0).getType() == SQLParser.ALL) {
      projectionOp.setAll();
    } else {
      CommonTree node;
      int numTargets = ast.getChildCount();
      Target [] targets = new Target[numTargets];
      Object evalTree;
      String alias;

      // the final one for each target is the alias
      // EBNF: bool_expr AS? fieldName
      for (int i = 0; i < ast.getChildCount(); i++) {
        node = (CommonTree) ast.getChild(i);
        evalTree = createExpression(node);
        targets[i] = new Target(evalTree);
        if (node.getChildCount() > 1) {
          alias = node.getChild(node.getChildCount() - 1).getChild(0).getText();
          targets[i].setAlias(alias);
        }
      }
      projectionOp.setTargets(targets);
    }

    return projectionOp;
  }

  /**
   * EBNF: table_list -> tableRef (COMMA tableRef)
   * @param ast
   */
  private RelationalOp parseFromClause(ParsingContext ctx, final CommonTree ast) {
    RelationalOp previous = null;
    CommonTree node;
    for (int i = 0; i < ast.getChildCount(); i++) {
      node = (CommonTree) ast.getChild(i);

      switch (node.getType()) {

        case SQLParser.TABLE:
          // table (AS ID)?
          // 0 - a table name, 1 - table alias
          if (previous != null) {
            RelationalOp inner = parseTable(node);
            JoinOp newJoin = new JoinOp(JoinType.INNER);
            newJoin.setOuter(previous);
            newJoin.setInner(inner);
            previous = newJoin;
          } else {
            previous = parseTable(node);
          }
          break;
        case SQLParser.JOIN:
          JoinOp newJoin = parseExplicitJoinClause(ctx, node);
          newJoin.setOuter(previous);
          previous = newJoin;
          break;
        default:
          throw new SQLSyntaxError(ctx.rawQuery, "Wrong From Clause");
      } // switch
    } // for each derievedTable

    return previous;
  }

  private Relation parseTable(final CommonTree tableAST) {
    String tableName = tableAST.getChild(0).getText();
    Relation table = new Relation(tableName);

    if (tableAST.getChildCount() > 1) {
      table.setAlias(tableAST.getChild(1).getText());
    }

    return table;
  }

  private JoinOp parseExplicitJoinClause(ParsingContext ctx, final CommonTree ast) {

    int idx = 0;
    int parsedJoinType = ast.getChild(idx).getType();

    JoinOp joinClause;

    switch (parsedJoinType) {
      case SQLParser.CROSS:
      case SQLParser.UNION:
        joinClause = parseCrossAndUnionJoin(ast);
        break;

      case SQLParser.NATURAL:
        joinClause = parseNaturalJoinClause(ctx, ast);
        break;

      case SQLParser.INNER:
      case SQLParser.OUTER:
        joinClause = parseQualifiedJoinClause(ctx, ast, 0);
        break;

      default: // default join (without join type) is inner join
        joinClause = parseQualifiedJoinClause(ctx, ast, 0);
    }

    return joinClause;
  }

  private JoinOp parseNaturalJoinClause(ParsingContext ctx, Tree ast) {
    JoinOp join = parseQualifiedJoinClause(ctx, ast, 1);
    join.setNatural();
    return join;
  }

  private JoinOp parseQualifiedJoinClause(ParsingContext ctx, final Tree ast, final int idx) {
    int childIdx = idx;
    JoinOp join = null;

    if (ast.getChild(childIdx).getType() == SQLParser.TABLE) { // default join
      join = new JoinOp(JoinType.INNER);
      join.setInner(parseTable((CommonTree) ast.getChild(childIdx)));

    } else {

      if (ast.getChild(childIdx).getType() == SQLParser.INNER) {
        join = new JoinOp(JoinType.INNER);

      } else if (ast.getChild(childIdx).getType() == SQLParser.OUTER) {

        switch (ast.getChild(childIdx).getChild(0).getType()) {
          case SQLParser.LEFT:
            join = new JoinOp(JoinType.LEFT_OUTER);
            break;
          case SQLParser.RIGHT:
            join = new JoinOp(JoinType.RIGHT_OUTER);
            break;
          case SQLParser.FULL:
            join = new JoinOp(JoinType.FULL_OUTER);
            break;
          default:
            throw new SQLSyntaxError(ctx.rawQuery, "Unknown Join Type");
        }
      }

      childIdx++;
      join.setInner(parseTable((CommonTree) ast.getChild(childIdx)));
    }

    childIdx++;

    if (ast.getChildCount() > childIdx) {
      CommonTree joinQual = (CommonTree) ast.getChild(childIdx);

      if (joinQual.getType() == SQLParser.ON) {
        Object joinCond = parseJoinCondition(joinQual);
        join.setJoinQual((Expr) joinCond);

      } else if (joinQual.getType() == SQLParser.USING) {
        ColumnRef [] joinColumns = parseJoinColumns(joinQual);
        join.setJoinColumns(joinColumns);
      }
    }

    return join;
  }

  private JoinOp parseCrossAndUnionJoin(Tree ast) {
    JoinType joinType;

    if (ast.getChild(0).getType() == SQLParser.CROSS) {
      joinType = JoinType.CROSS_JOIN;
    } else if (ast.getChild(0).getType() == SQLParser.UNION) {
      joinType = JoinType.UNION;
    } else {
      throw new IllegalStateException("Neither the AST has cross join or union join:\n"
          + ast.toStringTree());
    }

    JoinOp join = new JoinOp(joinType);
    Preconditions.checkState(ast.getChild(1).getType() == SQLParser.TABLE);
    join.setInner(parseTable((CommonTree) ast.getChild(1)));

    return join;
  }

  private ColumnRef [] parseJoinColumns(final CommonTree ast) {
    ColumnRef [] joinColumns = new ColumnRef[ast.getChildCount()];

    for (int i = 0; i < ast.getChildCount(); i++) {
      joinColumns[i] = checkAndGetColumnByAST(ast.getChild(i));
    }
    return joinColumns;
  }

  private Object parseJoinCondition(CommonTree ast) {
    return createExpression(ast.getChild(0));
  }

  private ColumnRef checkAndGetColumnByAST(final Tree fieldNode) {
    Preconditions.checkArgument(SQLParser.FIELD_NAME == fieldNode.getType());

    String columnName = fieldNode.getChild(0).getText();
    ColumnRef column = new ColumnRef(columnName);

    String tableName = null;
    if (fieldNode.getChildCount() > 1) {
      tableName = fieldNode.getChild(1).getText();
      column.setTableName(tableName);
    }

    return column;
  }

  /**
   * The EBNF of case statement
   * <pre>
   * searched_case
   * : CASE s=searched_when_clauses e=else_clause END -> ^(CASE $s $e)
   * ;
   *
   * searched_when_clauses
   * : searched_when_clause searched_when_clause* -> searched_when_clause+
   * ;
   *
   * searched_when_clause
   * : WHEN c=search_condition THEN r=result -> ^(WHEN $c $r)
   * ;
   *
   * else_clause
   * : ELSE r=result -> ^(ELSE $r)
   * ;
   * </pre>
   * @param tree
   * @return
   */
  public CaseWhenExpr parseCaseWhen(final Tree tree) {
    int idx = 0;

    CaseWhenExpr caseEval = new CaseWhenExpr();
    Expr cond;
    Expr thenResult;
    Tree when;

    for (; idx < tree.getChildCount() &&
        tree.getChild(idx).getType() == SQLParser.WHEN; idx++) {

      when = tree.getChild(idx);
      cond = (Expr) createExpression(when.getChild(0));
      thenResult = (Expr) createExpression(when.getChild(1));
      caseEval.addWhen(cond, thenResult);
    }

    if (tree.getChild(idx) != null &&
        tree.getChild(idx).getType() == SQLParser.ELSE) {
      Object elseResult = createExpression(tree.getChild(idx).getChild(0));
      caseEval.setElseResult(elseResult);
    }

    return caseEval;
  }

  /**
   * <pre>
   * like_predicate : fieldName NOT? LIKE string_value_expr
   * -> ^(LIKE NOT? fieldName string_value_expr)
   * </pre>
   * @param tree
   * @return
   */
  private LikeExpr parseLike(final Tree tree) {
    int idx = 0;

    boolean not = false;
    if (tree.getChild(idx).getType() == SQLParser.NOT) {
      not = true;
      idx++;
    }

    ColumnRef field = (ColumnRef) createExpression(tree.getChild(idx));
    idx++;
    Expr pattern = createExpression(tree.getChild(idx));

    return new LikeExpr(not, field, pattern);
  }

  public Expr createExpression(final Tree ast) {
    switch(ast.getType()) {

      // constants
      case SQLParser.Unsigned_Integer:
        return new LiteralExpr(ast.getText(), LiteralType.Unsigned_Integer);
      case SQLParser.Unsigned_Float:
        return new LiteralExpr(ast.getText(), LiteralType.Unsigned_Float);
      case SQLParser.Unsigned_Large_Integer:
        return new LiteralExpr(ast.getText(), LiteralType.Unsigned_Large_Integer);

      case SQLParser.Character_String_Literal:
        return new LiteralExpr(ast.getText(), LiteralType.String);

      // unary expression
      case SQLParser.NOT:
        ;

      // binary expressions
      case SQLParser.LIKE:
        return parseLike(ast);

      case SQLParser.IS:
        ;

      case SQLParser.AND:
      case SQLParser.OR:
        break;

      case SQLParser.Equals_Operator:
      case SQLParser.Not_Equals_Operator:
      case SQLParser.Less_Than_Operator:
      case SQLParser.Less_Or_Equals_Operator:
      case SQLParser.Greater_Than_Operator:
      case SQLParser.Greater_Or_Equals_Operator:
      case SQLParser.Plus_Sign:
      case SQLParser.Minus_Sign:
      case SQLParser.Asterisk:
      case SQLParser.Slash:
      case SQLParser.Percent:
        return new BinaryExpr(ExpressionType.stringToOperator(ast.getText()),
            createExpression(ast.getChild(0)),
            createExpression(ast.getChild(1)));

      // others
      case SQLParser.COLUMN:
        return createExpression(ast.getChild(0));

      case SQLParser.FIELD_NAME:
        return checkAndGetColumnByAST(ast);

      case SQLParser.FUNCTION:
        String signature = ast.getText();
        FunctionExpr func = new FunctionExpr(signature);
        Expr [] givenArgs = new Expr[ast.getChildCount()];

        for (int i = 0; i < ast.getChildCount(); i++) {
          givenArgs[i] = (Expr) createExpression(ast.getChild(i));
        }
        func.setArguments(givenArgs);

        break;
      case SQLParser.COUNT_VAL:

      case SQLParser.COUNT_ROWS:


      case SQLParser.CASE:
        return parseCaseWhen(ast);

      default:
    }
    return null;
  }
}
