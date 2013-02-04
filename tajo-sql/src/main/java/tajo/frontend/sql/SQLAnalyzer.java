package tajo.frontend.sql;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.Tree;
import tajo.algebra.Expression;
import tajo.algebra.ExpressionType;
import tajo.algebra.RelationalAlgebra;
import tajo.catalog.proto.CatalogProtos;
import tajo.engine.planner.PlanningContext;
import tajo.engine.query.exception.TQLParseError;

public class SQLAnalyzer {
  public RelationalAlgebra parse(String sql) {

    CommonTree tree = parseSQL(sql);
    return null;
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
      throw new TQLParseError(e.getMessage());
    }

    return ast;
  }

  private RelationalAlgebra transform(CommonTree ast) {

    switch (ast.getType()) {
      case SQLParser.SELECT:

      case SQLParser.UNION:

      case SQLParser.EXCEPT:

      case SQLParser.INTERSECT:

      case SQLParser.INSERT:

      case SQLParser.CREATE_INDEX:

      case SQLParser.CREATE_TABLE:

      case SQLParser.DROP_TABLE:

      case SQLParser.SHOW_TABLE:

      case SQLParser.DESC_TABLE:

      case SQLParser.SHOW_FUNCTION:

      default:
        return null;
    }
  }

  private RelationalAlgebra parseSelectStatement(PlanningContext context,
                                          final CommonTree ast) {

    RelationalAlgebra algebra = new RelationalAlgebra();

    CommonTree node;
    for (int cur = 0; cur < ast.getChildCount(); cur++) {
      node = (CommonTree) ast.getChild(cur);

      switch (node.getType()) {
        case SQLParser.FROM:
          break;

        case SQLParser.SET_QUALIFIER:
          break;

        case SQLParser.SEL_LIST:
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

    return null;
  }

  public Object createExpression(final Tree ast) {
    switch(ast.getType()) {

      // constants
      case SQLParser.Unsigned_Integer:
        return Integer.parseInt(ast.getText());
      case SQLParser.Unsigned_Float:
        return Float.parseFloat(ast.getText());
      case SQLParser.Unsigned_Large_Integer:
        return Long.parseLong(ast.getText());

      case SQLParser.Character_String_Literal:
        return ast.getText();

      // unary expression
      case SQLParser.NOT:
        ;

      // binary expressions
      case SQLParser.LIKE:
        ;

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
        return new Expression(ExpressionType.stringToOperator(ast.getText()),
            createExpression(ast.getChild(0)),
            createExpression(ast.getChild(1)));

      // others
      case SQLParser.COLUMN:

      case SQLParser.FIELD_NAME:

      case SQLParser.FUNCTION:
        String signature = ast.getText();

        Expression[] givenArgs = new Expression[ast.getChildCount()];
        CatalogProtos.DataType[] paramTypes = new CatalogProtos.DataType[ast.getChildCount()];

        for (int i = 0; i < ast.getChildCount(); i++) {
          givenArgs[i] = (Expression) createExpression(ast.getChild(i));
        }

        break;
      case SQLParser.COUNT_VAL:

      case SQLParser.COUNT_ROWS:


      case SQLParser.CASE:

      default:
    }
    return null;
  }
}
