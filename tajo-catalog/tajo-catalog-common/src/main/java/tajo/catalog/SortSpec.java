package tajo.catalog;

import com.google.gson.annotations.Expose;
import tajo.catalog.json.GsonCreator;

public class SortSpec implements Cloneable {
  @Expose private Column key;
  @Expose private boolean ascending = true;
  @Expose private boolean null_first = false;

  public SortSpec(final Column key) {
    this.key = key;
  }

  /**
   *
   * @param sortKey columns to sort
   * @param asc true if the sort order is ascending order
   * @param nullFirst
   * Otherwise, it should be false.
   */
  public SortSpec(final Column sortKey, final boolean asc,
                  final boolean nullFirst) {
    this(sortKey);
    this.ascending = asc;
    this.null_first = nullFirst;
  }

  public final boolean isAscending() {
    return this.ascending;
  }

  public final void setDescOrder() {
    this.ascending = false;
  }

  public final boolean isNullFirst() {
    return this.null_first;
  }

  public final void setNullFirst() {
    this.null_first = true;
  }

  public final Column getKey() {
    return this.key;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    SortSpec key = (SortSpec) super.clone();
    key.key = (Column) this.key.clone();
    key.ascending = ascending;

    return key;
  }

  public String toJSON() {
    key.mergeProtoToLocal();
    return GsonCreator.getInstance().toJson(this);
  }

  public String toString() {
    return "Sortkey (key="+ key
        + " "+(ascending ? "asc" : "desc")+")";
  }
}
