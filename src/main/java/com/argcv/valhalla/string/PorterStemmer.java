package com.argcv.valhalla.string;

/**
 * Another Impl Based On
 * http://snowball.tartarus.org/algorithms/porter/stemmer.html
 *
 * @author yu
 */
public class PorterStemmer {

    private final static Among a_0[] = {
            new Among("s", -1, 3),
            new Among("ies", 0, 2),
            new Among("sses", 0, 1),
            new Among("ss", 0, -1)
    };
    private final static Among a_1[] = {
            new Among("", -1, 3),
            new Among("bb", 0, 2),
            new Among("dd", 0, 2),
            new Among("ff", 0, 2),
            new Among("gg", 0, 2),
            new Among("bl", 0, 1),
            new Among("mm", 0, 2),
            new Among("nn", 0, 2),
            new Among("pp", 0, 2),
            new Among("rr", 0, 2),
            new Among("at", 0, 1),
            new Among("tt", 0, 2),
            new Among("iz", 0, 1)
    };
    private final static Among a_2[] = {
            new Among("ed", -1, 2),
            new Among("eed", 0, 1),
            new Among("ing", -1, 2)
    };
    private final static Among a_3[] = {
            new Among("anci", -1, 3),
            new Among("enci", -1, 2),
            new Among("abli", -1, 4),
            new Among("eli", -1, 6),
            new Among("alli", -1, 9),
            new Among("ousli", -1, 12),
            new Among("entli", -1, 5),
            new Among("aliti", -1, 10),
            new Among("biliti", -1, 14),
            new Among("iviti", -1, 13),
            new Among("tional", -1, 1),
            new Among("ational", 10, 8),
            new Among("alism", -1, 10),
            new Among("ation", -1, 8),
            new Among("ization", 13, 7),
            new Among("izer", -1, 7),
            new Among("ator", -1, 8),
            new Among("iveness", -1, 13),
            new Among("fulness", -1, 11),
            new Among("ousness", -1, 12)
    };
    private final static Among a_4[] = {
            new Among("icate", -1, 2),
            new Among("ative", -1, 3),
            new Among("alize", -1, 1),
            new Among("iciti", -1, 2),
            new Among("ical", -1, 2),
            new Among("ful", -1, 3),
            new Among("ness", -1, 3)
    };
    private final static Among a_5[] = {
            new Among("ic", -1, 1),
            new Among("ance", -1, 1),
            new Among("ence", -1, 1),
            new Among("able", -1, 1),
            new Among("ible", -1, 1),
            new Among("ate", -1, 1),
            new Among("ive", -1, 1),
            new Among("ize", -1, 1),
            new Among("iti", -1, 1),
            new Among("al", -1, 1),
            new Among("ism", -1, 1),
            new Among("ion", -1, 2),
            new Among("er", -1, 1),
            new Among("ous", -1, 1),
            new Among("ant", -1, 1),
            new Among("ent", -1, 1),
            new Among("ment", 15, 1),
            new Among("ement", 16, 1),
            new Among("ou", -1, 1)
    };
    private static final char g_v[] = {17, 65, 16, 1};
    private static final char g_v_WXY[] = {1, 17, 65, 208, 1};
    private StringBuffer current;
    private int limit;
    private int cursor;
    private int limit_backward;
    private int bra;
    private int ket;
    private int I_p2;
    private int I_p1;

    public PorterStemmer(String s) {
        current = new StringBuffer();
        setCurrent(s);
    }

//    private PorterStemmer() {
//        current = new StringBuffer();
//        setCurrent("");
//    }

    /**
     * Get the current string.
     */
    public String getCurrent() {
        String result = current.toString().trim();
        // Make a new StringBuffer.  If we reuse the old one, and a user of
        // the library keeps a reference to the buffer returned (for example,
        // by converting it to a String in a way which doesn't force a copy),
        // the buffer size will not decrease, and we will risk wasting a large
        // amount of memory.
        // Thanks to Wolfram Esser for spotting this problem.
        current = new StringBuffer();
        return result;
    }

    /**
     * Set the current string.
     */
    private void setCurrent(String value) {
        current.replace(0, current.length(), value);
        cursor = 0;
        limit = current.length();
        limit_backward = 0;
        bra = cursor;
        ket = limit;
    }

    private boolean in_grouping(char[] s, int min, int max) {
        if (cursor >= limit) return false;
        char ch = current.charAt(cursor);
        if (ch > max || ch < min) return false;
        ch -= min;
        if ((s[ch >> 3] & (0X1 << (ch & 0X7))) == 0) return false;
        cursor++;
        return true;
    }

    private boolean in_grouping_b(char[] s, int min, int max) {
        if (cursor <= limit_backward) return false;
        char ch = current.charAt(cursor - 1);
        if (ch > max || ch < min) return false;
        ch -= min;
        if ((s[ch >> 3] & (0X1 << (ch & 0X7))) == 0) return false;
        cursor--;
        return true;
    }

    private boolean out_grouping(char[] s, int min, int max) {
        if (cursor >= limit) return false;
        char ch = current.charAt(cursor);
        if (ch > max || ch < min) {
            cursor++;
            return true;
        }
        ch -= min;
        if ((s[ch >> 3] & (0X1 << (ch & 0X7))) == 0) {
            cursor++;
            return true;
        }
        return false;
    }

    private boolean out_grouping_b(char[] s, int min, int max) {
        if (cursor <= limit_backward) return false;
        char ch = current.charAt(cursor - 1);
        if (ch > max || ch < min) {
            cursor--;
            return true;
        }
        ch -= min;
        if ((s[ch >> 3] & (0X1 << (ch & 0X7))) == 0) {
            cursor--;
            return true;
        }
        return false;
    }

    private boolean eq_s(int s_size, String s) {
        if (limit - cursor < s_size) return false;
        int i;
        for (i = 0; i != s_size; i++) {
            if (current.charAt(cursor + i) != s.charAt(i)) return false;
        }
        cursor += s_size;
        return true;
    }

    private boolean eq_s_b(int s_size, String s) {
        if (cursor - limit_backward < s_size) return false;
        int i;
        for (i = 0; i != s_size; i++) {
            if (current.charAt(cursor - s_size + i) != s.charAt(i)) return false;
        }
        cursor -= s_size;
        return true;
    }

    // find_among_b is for backwards processing. Same comments apply
    private int find_among_b(Among v[], int v_size) {
        int i = 0;
        int j = v_size;

        int c = cursor;
        int lb = limit_backward;

        int common_i = 0;
        int common_j = 0;

        boolean first_key_inspected = false;

        while (true) {
            int k = i + ((j - i) >> 1);
            int diff = 0;
            int common = common_i < common_j ? common_i : common_j;
            Among w = v[k];
            int i2;
            for (i2 = w.s_size - 1 - common; i2 >= 0; i2--) {
                if (c - common == lb) {
                    diff = -1;
                    break;
                }
                diff = current.charAt(c - 1 - common) - w.s[i2];
                if (diff != 0) break;
                common++;
            }
            if (diff < 0) {
                j = k;
                common_j = common;
            } else {
                i = k;
                common_i = common;
            }
            if (j - i <= 1) {
                if (i > 0) break;
                if (j == i) break;
                if (first_key_inspected) break;
                first_key_inspected = true;
            }
        }
        while (true) {
            Among w = v[i];
            if (common_i >= w.s_size) {
                cursor = c - w.s_size;
                return w.result;
            }
            i = w.substring_i;
            if (i < 0) return 0;
        }
    }

    /* to replace chars between c_bra and c_ket in current by the
     * chars in s.
     */
    private int replace_s(int c_bra, int c_ket, String s) {
        int adjustment = s.length() - (c_ket - c_bra);
        current.replace(c_bra, c_ket, s);
        limit += adjustment;
        if (cursor >= c_ket) cursor += adjustment;
        else if (cursor > c_bra) cursor = c_bra;
        return adjustment;
    }

//    protected void slice_check() {
//        if (bra < 0 ||
//                bra > ket ||
//                ket > limit ||
//                limit > current.length())   // this line could be removed
//        {
//            System.err.println("faulty slice operation");
//            // FIXME: report error somehow.
//        }
//    }

    private void slice_from(String s) {
        //slice_check();
        replace_s(bra, ket, s);
    }

    private void slice_del() {
        slice_from("");
    }

    private void insert(int c_bra, int c_ket, String s) {
        int adjustment = replace_s(c_bra, c_ket, s);
        if (c_bra <= bra) bra += adjustment;
        if (c_bra <= ket) ket += adjustment;
    }

//    protected void copy_from(PorterStemmer other) {
//        // current
//        B_Y_found = other.B_Y_found;
//        I_p2 = other.I_p2;
//        I_p1 = other.I_p1;
//
//        // super
//        current = other.current;
//        cursor = other.cursor;
//        limit = other.limit;
//        limit_backward = other.limit_backward;
//        bra = other.bra;
//        ket = other.ket;
//    }

    private boolean r_shortv() {
        // (, line 19
        return out_grouping_b(g_v_WXY, 89, 121) && in_grouping_b(g_v, 97, 121) && out_grouping_b(g_v, 97, 121);
    }

    private boolean r_R1() {
        return I_p1 <= cursor;
    }

    private boolean r_R2() {
        return I_p2 <= cursor;
    }

    private boolean r_Step_1a() {
        int among_var;
        // (, line 24
        // [, line 25
        ket = cursor;
        // substring, line 25
        among_var = find_among_b(a_0, 4);
        if (among_var == 0) {
            return false;
        }
        // ], line 25
        bra = cursor;
        switch (among_var) {
            case 1:
                // (, line 26
                // <-, line 26
                slice_from("ss");
                break;
            case 2:
                // (, line 27
                // <-, line 27
                slice_from("i");
                break;
            case 3:
                // (, line 29
                // delete, line 29
                slice_del();
                break;
        }
        return true;
    }

    private boolean r_Step_1b() {
        int among_var;
        int v_1;
        int v_3;
        int v_4;
        // (, line 33
        // [, line 34
        ket = cursor;
        // substring, line 34
        among_var = find_among_b(a_2, 3);
        if (among_var == 0) {
            return false;
        }
        // ], line 34
        bra = cursor;
        switch (among_var) {
            case 1:
                // (, line 35
                // call R1, line 35
                if (!r_R1()) {
                    return false;
                }
                // <-, line 35
                slice_from("ee");
                break;
            case 2:
                // (, line 37
                // test, line 38
                v_1 = limit - cursor;
                // gopast, line 38
                // golab0:
                while (true) {
//                    do {
//                        if (!(in_grouping_b(g_v, 97, 121))) break;
//                        break golab0;
//                    } while (false);
                    if ((in_grouping_b(g_v, 97, 121))) break;
                    if (cursor <= limit_backward) {
                        return false;
                    }
                    cursor--;
                }
                cursor = limit - v_1;
                // delete, line 38
                slice_del();
                // test, line 39
                v_3 = limit - cursor;
                // substring, line 39
                among_var = find_among_b(a_1, 13);
                if (among_var == 0) {
                    return false;
                }
                cursor = limit - v_3;
                switch (among_var) {
                    case 1:
                        // (, line 41
                        // <+, line 41
                    {
                        int c = cursor;
                        insert(cursor, cursor, "e");
                        cursor = c;
                    }
                    break;
                    case 2:
                        // (, line 44
                        // [, line 44
                        ket = cursor;
                        // next, line 44
                        if (cursor <= limit_backward) {
                            return false;
                        }
                        cursor--;
                        // ], line 44
                        bra = cursor;
                        // delete, line 44
                        slice_del();
                        break;
                    case 3:
                        // (, line 45
                        // atmark, line 45
                        if (cursor != I_p1) {
                            return false;
                        }
                        // test, line 45
                        v_4 = limit - cursor;
                        // call shortv, line 45
                        if (!r_shortv()) {
                            return false;
                        }
                        cursor = limit - v_4;
                        // <+, line 45
                    {
                        int c = cursor;
                        insert(cursor, cursor, "e");
                        cursor = c;
                    }
                    break;
                }
                break;
        }
        return true;
    }

    private boolean r_Step_1c() {
        int v_1;
        // (, line 51
        // [, line 52
        ket = cursor;
        // or, line 52
        //lab0:
        do {
            v_1 = limit - cursor;
//            do {
//                // literal, line 52
//                if (!(eq_s_b(1, "y"))) {
//                    break;
//                }
//                break lab0;
//            } while (false);
            if (eq_s_b(1, "y")) break;
            cursor = limit - v_1;
            // literal, line 52
            if (!(eq_s_b(1, "Y"))) {
                return false;
            }
        } while (false);
        // ], line 52
        bra = cursor;
        // gopast, line 53
        // golab2:
        while (true) {
//            do {
//                if (!(in_grouping_b(g_v, 97, 121))) {
//                    break;
//                }
//                break golab2;
//            } while (false);
            if (in_grouping_b(g_v, 97, 121)) break;
            if (cursor <= limit_backward) {
                return false;
            }
            cursor--;
        }
        // <-, line 54
        slice_from("i");
        return true;
    }

    private boolean r_Step_2() {
        int among_var;
        // (, line 57
        // [, line 58
        ket = cursor;
        // substring, line 58
        among_var = find_among_b(a_3, 20);
        if (among_var == 0) {
            return false;
        }
        // ], line 58
        bra = cursor;
        // call R1, line 58
        if (!r_R1()) {
            return false;
        }
        switch (among_var) {
            case 1:
                // (, line 59
                // <-, line 59
                slice_from("tion");
                break;
            case 2:
                // (, line 60
                // <-, line 60
                slice_from("ence");
                break;
            case 3:
                // (, line 61
                // <-, line 61
                slice_from("ance");
                break;
            case 4:
                // (, line 62
                // <-, line 62
                slice_from("able");
                break;
            case 5:
                // (, line 63
                // <-, line 63
                slice_from("ent");
                break;
            case 6:
                // (, line 64
                // <-, line 64
                slice_from("e");
                break;
            case 7:
                // (, line 66
                // <-, line 66
                slice_from("ize");
                break;
            case 8:
                // (, line 68
                // <-, line 68
                slice_from("ate");
                break;
            case 9:
                // (, line 69
                // <-, line 69
                slice_from("al");
                break;
            case 10:
                // (, line 71
                // <-, line 71
                slice_from("al");
                break;
            case 11:
                // (, line 72
                // <-, line 72
                slice_from("ful");
                break;
            case 12:
                // (, line 74
                // <-, line 74
                slice_from("ous");
                break;
            case 13:
                // (, line 76
                // <-, line 76
                slice_from("ive");
                break;
            case 14:
                // (, line 77
                // <-, line 77
                slice_from("ble");
                break;
        }
        return true;
    }

    private boolean r_Step_3() {
        int among_var;
        // (, line 81
        // [, line 82
        ket = cursor;
        // substring, line 82
        among_var = find_among_b(a_4, 7);
        if (among_var == 0) {
            return false;
        }
        // ], line 82
        bra = cursor;
        // call R1, line 82
        if (!r_R1()) {
            return false;
        }
        switch (among_var) {
            case 1:
                // (, line 83
                // <-, line 83
                slice_from("al");
                break;
            case 2:
                // (, line 85
                // <-, line 85
                slice_from("ic");
                break;
            case 3:
                // (, line 87
                // delete, line 87
                slice_del();
                break;
        }
        return true;
    }

    private boolean r_Step_4() {
        int among_var;
        int v_1;
        // (, line 91
        // [, line 92
        ket = cursor;
        // substring, line 92
        among_var = find_among_b(a_5, 19);
        if (among_var == 0) {
            return false;
        }
        // ], line 92
        bra = cursor;
        // call R2, line 92
        if (!r_R2()) {
            return false;
        }
        switch (among_var) {
            case 1:
                // (, line 95
                // delete, line 95
                slice_del();
                break;
            case 2:
                // (, line 96
                // or, line 96
                //lab0:
                do {
                    v_1 = limit - cursor;
//                    do {
//                        // literal, line 96
//                        if (!(eq_s_b(1, "s"))) {
//                            break;
//                        }
//                        break lab0;
//                    } while (false);
                    if (eq_s_b(1, "s")) break;
                    cursor = limit - v_1;
                    // literal, line 96
                    if (!(eq_s_b(1, "t"))) {
                        return false;
                    }
                } while (false);
                // delete, line 96
                slice_del();
                break;
        }
        return true;
    }

    private boolean r_Step_5a() {
        int v_1;
        int v_2;
        // (, line 100
        // [, line 101
        ket = cursor;
        // literal, line 101
        if (!(eq_s_b(1, "e"))) {
            return false;
        }
        // ], line 101
        bra = cursor;
        // or, line 102
//        lab0:
        do {
            v_1 = limit - cursor;
//            do {
//                // call R2, line 102
//                if (!r_R2()) {
//                    break;
//                }
//                break lab0;
//            } while (false);
            if (r_R2()) break;
            cursor = limit - v_1;
            // (, line 102
            // call R1, line 102
            if (!r_R1()) {
                return false;
            }
            // not, line 102
            {
                v_2 = limit - cursor;
                if (r_shortv()) {
                    return false;
                }
                cursor = limit - v_2;
            }
        } while (false);
        // delete, line 103
        slice_del();
        return true;
    }

    private boolean r_Step_5b() {
        // (, line 106
        // [, line 107
        ket = cursor;
        // literal, line 107
        if (!(eq_s_b(1, "l"))) {
            return false;
        }
        // ], line 107
        bra = cursor;
        // call R2, line 108
        if (!r_R2()) {
            return false;
        }
        // literal, line 108
        if (!(eq_s_b(1, "l"))) {
            return false;
        }
        // delete, line 109
        slice_del();
        return true;
    }

    public boolean stem() {
        int v_1;
        int v_2;
        int v_3;
        int v_4;
        int v_5;
        int v_10;
        int v_11;
        int v_12;
        int v_13;
        int v_14;
        int v_15;
        int v_16;
        int v_17;
        int v_18;
        int v_19;
        int v_20;
        // (, line 113
        // unset Y_found, line 115
        boolean b_Y_found = false;
        // do, line 116
        v_1 = cursor;
        do {
            // (, line 116
            // [, line 116
            bra = cursor;
            // literal, line 116
            if (!(eq_s(1, "y"))) {
                break;
            }
            // ], line 116
            ket = cursor;
            // <-, line 116
            slice_from("Y");
            // set Y_found, line 116
            b_Y_found = true;
        } while (false);
        cursor = v_1;
        // do, line 117
        v_2 = cursor;
        do {
            // repeat, line 117
            replab2:
            while (true) {
                v_3 = cursor;
                lab3:
                do {
                    // (, line 117
                    // goto, line 117
                    while (true) {
                        v_4 = cursor;
                        if (in_grouping(g_v, 97, 121)) {
                            bra = cursor;
                            if (eq_s(1, "y")) {
                                ket = cursor;
                                cursor = v_4;
                                break;
                            }
                        }
                        cursor = v_4;
                        if (cursor >= limit) {
                            break lab3;
                        }
                        cursor++;
                    }
                    // <-, line 117
                    slice_from("Y");
                    // set Y_found, line 117
                    b_Y_found = true;
                    continue replab2;
                } while (false);
                cursor = v_3;
                break;
            }
        } while (false);
        cursor = v_2;
        I_p1 = limit;
        I_p2 = limit;
        // do, line 121
        v_5 = cursor;
        lab6:
        do {
            // (, line 121
            // gopast, line 122
            while (true) {
                if (in_grouping(g_v, 97, 121)) {
                    break;
                }
                if (cursor >= limit) {
                    break lab6;
                }
                cursor++;
            }
            // gopast, line 122
            while (true) {
                if ((out_grouping(g_v, 97, 121))) {
                    break;
                }
                if (cursor >= limit) {
                    break lab6;
                }
                cursor++;
            }
            // setmark p1, line 122
            I_p1 = cursor;
            // gopast, line 123
            while (true) {
                if (in_grouping(g_v, 97, 121)) {
                    break;
                }
                if (cursor >= limit) {
                    break lab6;
                }
                cursor++;
            }
            // gopast, line 123
            while (true) {
                if (out_grouping(g_v, 97, 121)) {
                    break;
                }
                if (cursor >= limit) {
                    break lab6;
                }
                cursor++;
            }
            // setmark p2, line 123
            I_p2 = cursor;
        } while (false);
        cursor = v_5;
        // backwards, line 126
        limit_backward = cursor;
        cursor = limit;
        // (, line 126
        // do, line 127
        v_10 = limit - cursor;
        do {
            // call Step_1a, line 127
            if (!r_Step_1a()) {
                break;
            }
        } while (false);
        cursor = limit - v_10;
        // do, line 128
        v_11 = limit - cursor;
        do {
            // call Step_1b, line 128
            if (!r_Step_1b()) {
                break;
            }
        } while (false);
        cursor = limit - v_11;
        // do, line 129
        v_12 = limit - cursor;
        do {
            // call Step_1c, line 129
            if (!r_Step_1c()) {
                break;
            }
        } while (false);
        cursor = limit - v_12;
        // do, line 130
        v_13 = limit - cursor;
        do {
            // call Step_2, line 130
            if (!r_Step_2()) {
                break;
            }
        } while (false);
        cursor = limit - v_13;
        // do, line 131
        v_14 = limit - cursor;
        do {
            // call Step_3, line 131
            if (!r_Step_3()) {
                break;
            }
        } while (false);
        cursor = limit - v_14;
        // do, line 132
        v_15 = limit - cursor;
        do {
            // call Step_4, line 132
            if (!r_Step_4()) {
                break;
            }
        } while (false);
        cursor = limit - v_15;
        // do, line 133
        v_16 = limit - cursor;
        do {
            // call Step_5a, line 133
            if (!r_Step_5a()) {
                break;
            }
        } while (false);
        cursor = limit - v_16;
        // do, line 134
        v_17 = limit - cursor;
        do {
            // call Step_5b, line 134
            if (!r_Step_5b()) {
                break;
            }
        } while (false);
        cursor = limit - v_17;
        cursor = limit_backward;                    // do, line 137
        v_18 = cursor;
        do {
            // (, line 137
            // Boolean test Y_found, line 137
            if (!(b_Y_found)) {
                break;
            }
            // repeat, line 137
            replab24:
            while (true) {
                v_19 = cursor;
                lab25:
                do {
                    // (, line 137
                    // goto, line 137
                    while (true) {
                        v_20 = cursor;
                        bra = cursor;
                        if (eq_s(1, "Y")) {
                            ket = cursor;
                            cursor = v_20;
                            break;
                        }
                        cursor = v_20;
                        if (cursor >= limit) {
                            break lab25;
                        }
                        cursor++;
                    }
                    // <-, line 137
                    slice_from("y");
                    continue replab24;
                } while (false);
                cursor = v_19;
                break;
            }
        } while (false);
        cursor = v_18;
        return true;
    }

    private static class Among {
        public final char[] s; /* search string */
        final int result; /* result of the lookup */
        final int s_size; /* search string */
        final int substring_i; /* index to longest matching substring */

        Among(String s, int substring_i, int result) {
            this.s_size = s.length();
            this.s = s.toCharArray();
            this.substring_i = substring_i;
            this.result = result;
        }
    }
}
