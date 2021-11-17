package replication;

class Pair<L, R> {

    private final L left;
    private final R right;

    Pair(final L left, final R right) {
        this.left = left;
        this.right = right;
    }

    public L getLeft() {
        return left;
    }

    public R getRight() {
        return right;
    }

    public static <L, R> Pair<L, R> of(final L left, final R right) {
        return new Pair<>(left, right);
    }
}

class Triple<L, M, R> extends Pair<L, R> {
    private final M middle;

    Triple(final L left, final M middle, final R right) {
        super(left, right);
        this.middle = middle;
    }

    public M getMiddle() {
        return middle;
    }

    public static <L, M, R> Triple<L, M, R> of(final L left, final M middle, final R right) {
        return new Triple<>(left, middle, right);
    }

}