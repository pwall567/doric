package net.pwall.util;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class SortedSet<E extends Comparable<E>> implements Set<E> {

    private List<E> list;

    public SortedSet() {
        list = new ArrayList<>();
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
        E target = (E)o;
        int lo = 0;
        int hi = list.size();
        while (lo < hi) {
            int mid = (lo + hi) >> 1;
            int compare = list.get(mid).compareTo(target);
            if (compare == 0)
                return true;
            if (compare < 0)
                lo = mid + 1;
            else
                hi = mid;
        }
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        return list.iterator();
    }

    @Override
    public Object[] toArray() {
        return list.toArray();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T[] toArray(T[] a) {
        int listLen = list.size();
        int arrayLen = a.length;
        int i = 0;
        T[] dest = a;
        if (arrayLen < listLen) {
            dest = (T[])Array.newInstance(a.getClass().getComponentType(), listLen);
            arrayLen = listLen;
        }
        while (i < listLen) {
            dest[i] = (T)list.get(i);
            i++;
        }
        while (i < arrayLen)
            dest[i++] = null;
        return dest;
    }

    @Override
    public boolean add(E e) {
        int lo = 0;
        int hi = list.size();
        while (lo < hi) {
            int mid = (lo + hi) >> 1;
            int comp = list.get(mid).compareTo(e);
            if (comp == 0)
                return false;
            if (comp < 0)
                lo = mid + 1;
            else
                hi = mid;
        }
        list.add(lo, e);
        return true;
    }

    @Override
    public boolean remove(Object o) {
        @SuppressWarnings("unchecked")
        E c = (E)o;
        int lo = 0;
        int hi = list.size();
        while (lo < hi) {
            int mid = (lo + hi) >> 1;
            int comp = list.get(mid).compareTo(c);
            if (comp == 0) {
                list.remove(mid);
                return true;
            }
            if (comp < 0)
                lo = mid + 1;
            else
                hi = mid;
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object a : c)
            if (!contains(a))
                return false;
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean added = false;
        for (E a : c)
            added |= add(a);
        return added;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        boolean removed = false;
        for (E a : list)
            if (!c.contains(a))
                removed |= list.remove(a);
        return removed;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean removed = false;
        for (E a : list)
            if (c.contains(a))
                removed |= list.remove(a);
        return removed;
    }

    @Override
    public void clear() {
        list.clear();
    }

}
