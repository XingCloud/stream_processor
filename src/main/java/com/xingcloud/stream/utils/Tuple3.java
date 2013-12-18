package com.xingcloud.stream.utils;

/**
 * User: liuxiong
 * Date: 13-12-18
 * Time: 上午10:44
 */
public final class Tuple3<FIRST, SECOND, THIRD> {

  public final FIRST first;
  public final SECOND second;
  public final THIRD third;

  public Tuple3(FIRST first, SECOND second, THIRD third) {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Tuple3<?, ?, ?>)) {
      return false;
    }

    Tuple3<?, ?, ?> that = (Tuple3<?, ?, ?>)obj;

    return equal(this.first, that.first)
      && equal(this.second, that.second)
      && equal(this.third, that.third);
  }

  private static boolean equal(Object a, Object b) {
    return a == b || (a != null && a.equals(b));
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 37 * result + (first != null ? first.hashCode() : 0);
    result = 37 * result + (second != null ? second.hashCode() : 0);
    result = 37 * result + (third != null ? third.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return String.format("(%s, %s, %s)", first, second, third);
  }

  public static void main(String... args) {
    final String first = "test";
    final Long second = 20131218L;
    final String third = "a.b.c.d.";
    Tuple3<String, Long, String> t1 = new Tuple3<String, Long, String>(first, second, third);
    Tuple3<String, Long, String> t2 = new Tuple3<String, Long, String>(first, second, third);

    System.out.println(t1.hashCode() + "\n" + t2.hashCode() + "\n" + t1.equals(t2) + "\n" + t1.equals(t1));
  }

}
