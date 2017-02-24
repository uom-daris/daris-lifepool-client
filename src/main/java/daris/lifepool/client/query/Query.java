package daris.lifepool.client.query;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.pixelmed.dicom.AttributeTag;

import arc.mf.client.ServerClient;
import arc.xml.XmlStringWriter;

public class Query {

    private LinkedHashMap<AttributeTag, QueryElement> _elements;
    private boolean _excludeNullElement = true;

    Query(List<QueryElement> elements) {
        _elements = new LinkedHashMap<AttributeTag, QueryElement>();
        if (elements != null) {
            for (QueryElement element : elements) {
                putElement(element);
            }
        }
    }

    Query() {
        this(null);
    }

    public void putElement(QueryElement element) {
        if (element != null) {
            _elements.put(element.attributeTag(), element);
        }
    }

    public void putElement(AttributeTag tag, Predicate... predicates) {
        if (predicates == null || predicates.length == 0) {
            if (_elements.containsKey(tag)) {
                _elements.remove(tag);
            }
            return;
        }
        putElement(new QueryElement(tag, predicates));
    }

    public void putElement(AttributeTag tag, Operator op, String value) {
        putElement(tag, new Predicate(null, op, value));
    }

    public void removeElement(AttributeTag tag) {
        _elements.remove(tag);
    }

    public void save(StringBuilder sb) {
        Collection<QueryElement> elements = _elements.values();
        if (elements != null && !elements.isEmpty()) {
            boolean isFirstElement = true;
            for (QueryElement element : elements) {
                if (isFirstElement) {
                    isFirstElement = false;
                } else {
                    sb.append(" ").append(LogicOperator.AND).append(" ");
                }
                element.save(sb, _excludeNullElement);
            }
        }
    }

    public boolean excludeNullElement() {
        return _excludeNullElement;
    }

    public void setExcludeNullElement(boolean excludeNullElement) {
        _excludeNullElement = excludeNullElement;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        save(sb);
        return sb.toString();
    }

    public void execute(ServerClient.Connection cxn, String pid, Set<String> resultAssetIds) throws Throwable {
        StringBuilder sb = new StringBuilder();
        sb.append("(cid starts with '").append(pid).append("') and (");
        sb.append(toString()).append(")");
        XmlStringWriter w = new XmlStringWriter();
        w.add("where", sb.toString());
        w.add("action", "get-id");
        w.add("size", "infinity");
        Collection<String> ids = cxn.execute("asset.query", w.document()).values("id");
        if (ids != null && !ids.isEmpty()) {
            resultAssetIds.addAll(ids);
        }
    }

    /**
     * Executes the queries and the result is a query string that includes all
     * the result asset ids. e.g. "id=1 or id=2 or id=5"
     * 
     * @param cxn
     * @param pid
     * @param queries
     * @return
     * @throws Throwable
     */
    public static String execute(ServerClient.Connection cxn, String pid, List<Query> queries) throws Throwable {
        Set<String> resultAssetIds = execute(pid, queries, cxn);
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String id : resultAssetIds) {
            if (first) {
                first = false;
            } else {
                sb.append(" or ");
            }
            sb.append("id=").append(id);
        }
        return sb.toString();
    }

    /**
     * Executes the queries and returns a set of result asset ids.
     * 
     * @param pid
     * @param queries
     * @param cxn
     * @return
     * @throws Throwable
     */
    public static Set<String> execute(String pid, List<Query> queries, ServerClient.Connection cxn) throws Throwable {
        Set<String> resultAssetIds = new TreeSet<String>();
        for (Query query : queries) {
            query.execute(cxn, pid, resultAssetIds);
        }
        return resultAssetIds;
    }

}
