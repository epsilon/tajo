package tajo.frontend.sql;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import tajo.algebra.RelationalAlgebra;
import tajo.catalog.SortSpec;
import tajo.engine.parser.NQLParser;
import tajo.engine.parser.QueryBlock;
import tajo.engine.parser.StatementType;
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
}
