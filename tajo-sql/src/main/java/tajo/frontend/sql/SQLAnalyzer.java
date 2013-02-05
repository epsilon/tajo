package tajo.frontend.sql;

import com.google.common.base.Preconditions;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import tajo.algebra.*;
import tajo.algebra.Aggregation.GroupElement;
import tajo.algebra.Aggregation.GroupType;
import tajo.algebra.LiteralExpr.LiteralType;
import tajo.algebra.Sort.SortSpec;
import tajo.engine.parser.NQLParser;
import tajo.engine.planner.JoinType;

import java.util.ArrayList;
import java.util.List;

public class SQLAnalyzer {

  public Expr parse(String sql) {
    ParsingContext context = new ParsingContext(sql);
    QueryBlock queryBlock = new QueryBlock();
    CommonTree tree = parseSQL(sql);
    return transform(context, queryBlock, tree);
  }

  private static class ParsingContext {
    private String rawQuery;
    public ParsingContext(String sql) {
      this.rawQuery = sql;
    }
  }

  public static CommonTree parseSQL(String sql) {
    ANTLRStringStream input = new ANTLRStringStream(sql);
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

  private Expr transform(ParsingContext context, QueryBlock block, CommonTree ast) {

    switch (ast.getType()) {
      case SQLParser.SELECT:
        return parseSelectStatement(context, block, ast);
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

  private Expr parseSelectStatement(ParsingContext context,
                                            QueryBlock block,
                                            final CommonTree ast) {
    Expr relationOp = null;
    CommonTree node;
    for (int cur = 0; cur < ast.getChildCount(); cur++) {
      node = (CommonTree) ast.getChild(cur);

      switch (node.getType()) {
        case SQLParser.FROM:
          Expr fromClause = parseFromClause(context, node);
          block.setFromClause(fromClause);
          break;

        case SQLParser.SET_QUALIFIER:
          break;

        case SQLParser.SEL_LIST:
          Projection projection = parseSelectList(node);
          block.setProjection(projection);
          break;

        case SQLParser.WHERE:
          block.setSearchCondition(parseWhereClause(node));
          break;

        case SQLParser.GROUP_BY:
          Aggregation aggregation = parseGroupByClause(node);
          block.setGroupbyClause(aggregation);
          break;

        case SQLParser.HAVING:
          break;

        case SQLParser.ORDER_BY:
          SortSpec [] sortSpecs = parseSortSpecifiers(node.getChild(0));
          Sort sort = new Sort(sortSpecs);
          break;

        case SQLParser.LIMIT:

          break;

        default:

      }
    }

    System.out.println("block");

    return relationOp;
  }

  /**
   * Should be given SortSpecifiers Node
   *
   * EBNF: sort_specifier (COMMA sort_specifier)* -> sort_specifier+
   *
   * @param ast
   */
  private SortSpec[] parseSortSpecifiers(final Tree ast) {
    int numSortKeys = ast.getChildCount();
    SortSpec[] sortSpecs = new SortSpec[numSortKeys];
    CommonTree node;
    ColumnRefExpr column;

    // Each child has the following EBNF and AST:
    // EBNF: fn=fieldName a=order_specification? o=null_ordering?
    // AST: ^(SORT_KEY $fn $a? $o?)
    for (int i = 0; i < numSortKeys; i++) {
      node = (CommonTree) ast.getChild(i);
      column = checkAndGetColumnByAST(node.getChild(0));
      sortSpecs[i] = new SortSpec(column);

      if (node.getChildCount() > 1) {
        Tree child;
        for (int j = 1; j < node.getChildCount(); j++) {
          child = node.getChild(j);

          // AST: ^(ORDER ASC) | ^(ORDER DESC)
          if (child.getType() == NQLParser.ORDER) {
            if (child.getChild(0).getType() == NQLParser.DESC) {
              sortSpecs[i].setDescending();
            }
          } else if (child.getType() == NQLParser.NULL_ORDER) {
            // AST: ^(NULL_ORDER FIRST) | ^(NULL_ORDER LAST)
            if (child.getChild(0).getType() == NQLParser.FIRST) {
              sortSpecs[i].setNullFirst();
            }
          }
        }
      }
    }

    return sortSpecs;
  }

  /**
   * See 'groupby_clause' rule in SQL.g
   *
   * @param ast
   */
  private Aggregation parseGroupByClause(final CommonTree ast) {
    int idx = 0;
    Aggregation clause = new Aggregation();
    if (ast.getChild(idx).getType() == SQLParser.EMPTY_GROUPING_SET) {

    } else {
      // the remain ones are grouping fields.
      Tree group;
      List<ColumnRefExpr> columnRefs = new ArrayList<>();
      ColumnRefExpr [] columns;
      ColumnRefExpr column;
      for (; idx < ast.getChildCount(); idx++) {
        group = ast.getChild(idx);
        switch (group.getType()) {
          case NQLParser.CUBE:
            columns = parseColumnReferences((CommonTree) group);
            GroupElement cube = new GroupElement(GroupType.CUBE, columns);
            clause.addGroupSet(cube);
            break;

          case NQLParser.ROLLUP:
            columns = parseColumnReferences((CommonTree) group);
            GroupElement rollup = new GroupElement(GroupType.ROLLUP, columns);
            clause.addGroupSet(rollup);
            break;

          case NQLParser.FIELD_NAME:
            column = checkAndGetColumnByAST(group);
            columnRefs.add(column);
            break;
        }
      }

      if (columnRefs.size() > 0) {
        ColumnRefExpr [] groupingFields = columnRefs.toArray(new ColumnRefExpr[columnRefs.size()]);
        GroupElement g = new GroupElement(GroupType.GROUPBY, groupingFields);
        clause.addGroupSet(g);
      }
    }
    return clause;
  }



  /**
   * It parses the below EBNF.
   * <pre>
   * column_reference
   * : fieldName (COMMA fieldName)* -> fieldName+
   * ;
   * </pre>
   * @param parent
   * @return
   */
  private ColumnRefExpr [] parseColumnReferences(final CommonTree parent) {
    ColumnRefExpr [] columns = new ColumnRefExpr[parent.getChildCount()];

    for (int i = 0; i < columns.length; i++) {
      columns[i] = checkAndGetColumnByAST(parent.getChild(i));
    }

    return columns;
  }

  private Expr parseWhereClause(final CommonTree ast) {
    return createExpression(ast.getChild(0));
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
  private Projection parseSelectList(final CommonTree ast) {
    Projection projection = new Projection();
    if (ast.getChild(0).getType() == SQLParser.ALL) {
      projection.setAll();
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
      projection.setTargets(targets);
    }

    return projection;
  }

  /**
   * EBNF: table_list -> tableRef (COMMA tableRef)
   * @param ast
   */
  private Expr parseFromClause(ParsingContext ctx, final CommonTree ast) {
    Expr previous = null;
    CommonTree node;
    for (int i = 0; i < ast.getChildCount(); i++) {
      node = (CommonTree) ast.getChild(i);

      switch (node.getType()) {

        case SQLParser.TABLE:
          // table (AS ID)?
          // 0 - a table name, 1 - table alias
          if (previous != null) {
            Expr inner = parseTable(node);
            Join newJoin = new Join(JoinType.INNER);
            newJoin.setLeft(previous);
            newJoin.setRight(inner);
            previous = newJoin;
          } else {
            previous = parseTable(node);
          }
          break;
        case SQLParser.JOIN:
          Join newJoin = parseExplicitJoinClause(ctx, node);
          newJoin.setLeft(previous);
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

  private Join parseExplicitJoinClause(ParsingContext ctx, final CommonTree ast) {

    int idx = 0;
    int parsedJoinType = ast.getChild(idx).getType();

    Join joinClause;

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

  private Join parseNaturalJoinClause(ParsingContext ctx, Tree ast) {
    Join join = parseQualifiedJoinClause(ctx, ast, 1);
    join.setNatural();
    return join;
  }

  private Join parseQualifiedJoinClause(ParsingContext ctx, final Tree ast, final int idx) {
    int childIdx = idx;
    Join join = null;

    if (ast.getChild(childIdx).getType() == SQLParser.TABLE) { // default join
      join = new Join(JoinType.INNER);
      join.setRight(parseTable((CommonTree) ast.getChild(childIdx)));

    } else {

      if (ast.getChild(childIdx).getType() == SQLParser.INNER) {
        join = new Join(JoinType.INNER);

      } else if (ast.getChild(childIdx).getType() == SQLParser.OUTER) {

        switch (ast.getChild(childIdx).getChild(0).getType()) {
          case SQLParser.LEFT:
            join = new Join(JoinType.LEFT_OUTER);
            break;
          case SQLParser.RIGHT:
            join = new Join(JoinType.RIGHT_OUTER);
            break;
          case SQLParser.FULL:
            join = new Join(JoinType.FULL_OUTER);
            break;
          default:
            throw new SQLSyntaxError(ctx.rawQuery, "Unknown Join Type");
        }
      }

      childIdx++;
      join.setRight(parseTable((CommonTree) ast.getChild(childIdx)));
    }

    childIdx++;

    if (ast.getChildCount() > childIdx) {
      CommonTree joinQual = (CommonTree) ast.getChild(childIdx);

      if (joinQual.getType() == SQLParser.ON) {
        Expr joinCond = parseJoinCondition(joinQual);
        join.setJoinQual(joinCond);

      } else if (joinQual.getType() == SQLParser.USING) {
        ColumnRefExpr[] joinColumns = parseJoinColumns(joinQual);
        join.setJoinColumns(joinColumns);
      }
    }

    return join;
  }

  private Join parseCrossAndUnionJoin(Tree ast) {
    JoinType joinType;

    if (ast.getChild(0).getType() == SQLParser.CROSS) {
      joinType = JoinType.CROSS_JOIN;
    } else if (ast.getChild(0).getType() == SQLParser.UNION) {
      joinType = JoinType.UNION;
    } else {
      throw new IllegalStateException("Neither the AST has cross join or union join:\n"
          + ast.toStringTree());
    }

    Join join = new Join(joinType);
    Preconditions.checkState(ast.getChild(1).getType() == SQLParser.TABLE);
    join.setRight(parseTable((CommonTree) ast.getChild(1)));

    return join;
  }

  private ColumnRefExpr[] parseJoinColumns(final CommonTree ast) {
    ColumnRefExpr[] joinColumns = new ColumnRefExpr[ast.getChildCount()];

    for (int i = 0; i < ast.getChildCount(); i++) {
      joinColumns[i] = checkAndGetColumnByAST(ast.getChild(i));
    }
    return joinColumns;
  }

  private Expr parseJoinCondition(CommonTree ast) {
    return createExpression(ast.getChild(0));
  }

  private ColumnRefExpr checkAndGetColumnByAST(final Tree fieldNode) {
    Preconditions.checkArgument(SQLParser.FIELD_NAME == fieldNode.getType());

    String columnName = fieldNode.getChild(0).getText();
    ColumnRefExpr column = new ColumnRefExpr(columnName);

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
      cond = createExpression(when.getChild(0));
      thenResult = createExpression(when.getChild(1));
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

    ColumnRefExpr field = (ColumnRefExpr) createExpression(tree.getChild(idx));
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
        return new BinaryExpr(tokenToExprType(ast.getType()),
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
        Expr[] givenArgs = new Expr[ast.getChildCount()];

        for (int i = 0; i < ast.getChildCount(); i++) {
          givenArgs[i] = (Expr) createExpression(ast.getChild(i));
        }
        func.setParams(givenArgs);

        break;
      case SQLParser.COUNT_VAL:

      case SQLParser.COUNT_ROWS:


      case SQLParser.CASE:
        return parseCaseWhen(ast);

      default:
    }
    return null;
  }

  public static ExprType tokenToExprType(int tokenId) {
    switch (tokenId) {
      case SQLParser.Equals_Operator: return ExprType.Equals;
      case SQLParser.Less_Than_Operator: return ExprType.LessThan;
      case SQLParser.Less_Or_Equals_Operator: return ExprType.LessThan;
      case SQLParser.Greater_Than_Operator: return ExprType.GreaterThan;
      case SQLParser.Greater_Or_Equals_Operator: return ExprType.GreaterThanOrEquals;
      case SQLParser.Plus_Sign: return ExprType.Plus;
      case SQLParser.Minus_Sign: return ExprType.Minus;
      case SQLParser.Asterisk: return ExprType.Multiply;
      case SQLParser.Slash: return ExprType.Divide;
      case SQLParser.Percent: return ExprType.Mod;

      default: throw new RuntimeException("Unknown Token Id: " + tokenId);
    }
  }
}
