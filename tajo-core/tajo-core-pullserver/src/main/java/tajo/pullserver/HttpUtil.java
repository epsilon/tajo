package tajo.pullserver;

import com.google.common.collect.Maps;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Map;

/**
 * @author  Hyunsik Choi
 */
public class HttpUtil {
  public static Map<String,String> getParams(URI uri) throws UnsupportedEncodingException {
    return getParamsFromQuery(uri.getQuery());
  }

  /**
   * It parses a query string into key/value pairs
   *
   * @param queryString decoded query string
   * @return key/value pairs parsed from a given query string
   * @throws java.io.UnsupportedEncodingException
   */
  public static Map<String, String> getParamsFromQuery(String queryString) throws UnsupportedEncodingException {
    String [] queries = queryString.split("&");

    Map<String,String> params = Maps.newHashMap();
    String [] param;
    for (String q : queries) {
      param = q.split("=");
      params.put(param[0], param[1]);
    }

    return params;
  }

  public static String buildQuery(Map<String,String> params) throws UnsupportedEncodingException {
    StringBuilder sb = new StringBuilder();

    boolean first = true;
    for (Map.Entry<String,String> param : params.entrySet()) {
      if (!first) {
        sb.append("&");
      }
      sb.append(URLEncoder.encode(param.getKey(), "UTF-8")).
          append("=").
          append(URLEncoder.encode(param.getValue(), "UTF-8"));
      first = false;
    }

    return sb.toString();
  }
}
