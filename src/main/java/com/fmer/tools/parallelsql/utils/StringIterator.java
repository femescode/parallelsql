package com.fmer.tools.parallelsql.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 字符串迭代器
 * @author fengmeng
 * @date 2021/1/10 12:32
 */
public class StringIterator {
    private String iteratorStr;
    private int iteratorIndex;
    private String matchStr;
    private Matcher matcher;
    private StringBuilder sb;

    public StringIterator(String iteratorStr) {
        this.iteratorStr = iteratorStr;
        this.iteratorIndex = 0;
    }

    public boolean startsWith(String matchStr) {
        if (this.iteratorStr.startsWith(matchStr, this.iteratorIndex)) {
            this.matchStr = matchStr;
            return true;
        } else {
            return false;
        }
    }

    public boolean startsWithRegex(String regex) {
        Pattern p = Pattern.compile("\\G" + regex);
        if (this.matcher == null) {
            this.matcher = p.matcher(this.iteratorStr);
        } else {
            this.matcher.usePattern(p);
        }

        if (this.matcher.find(this.iteratorIndex)) {
            this.matchStr = this.matcher.group();
            return true;
        } else {
            return false;
        }
    }

    public String getMatchStr() {
        return this.matchStr;
    }

    public String moveMatch() {
        this.iteratorIndex += this.matchStr.length();
        return this.matchStr;
    }

    public char moveChar() {
        char c = this.iteratorStr.charAt(this.iteratorIndex);
        ++this.iteratorIndex;
        return c;
    }

    public String moveToCloseAfter(String close, String... ignoreStrs) {
        int findIndex = this.findCloseIndex(this.iteratorIndex + this.matchStr.length(), close, ignoreStrs);
        if (findIndex < 0) {
            return null;
        } else {
            int endIndex = findIndex + close.length();
            String moveStr = this.iteratorStr.substring(this.iteratorIndex, endIndex);
            this.iteratorIndex = endIndex;
            return moveStr;
        }
    }

    public String moveToClose(String close, String... ignoreStrs) {
        int findIndex = this.findCloseIndex(this.iteratorIndex + this.matchStr.length(), close, ignoreStrs);
        if (findIndex < 0) {
            return null;
        } else {
            String moveStr = this.iteratorStr.substring(this.iteratorIndex, findIndex);
            this.iteratorIndex = findIndex;
            return moveStr;
        }
    }

    private int findCloseIndex(int currentIteratorIndex, String close, String... ignoreStrs) {
        int findIndex = -1;

        while(this.canMove()) {
            boolean isMatchIgnoreStrs = false;
            String[] var6 = ignoreStrs;
            int var7 = ignoreStrs.length;

            for(int var8 = 0; var8 < var7; ++var8) {
                String ignoreStr = var6[var8];
                if (this.iteratorStr.startsWith(ignoreStr, currentIteratorIndex)) {
                    currentIteratorIndex += ignoreStr.length();
                    isMatchIgnoreStrs = true;
                    break;
                }
            }

            if (!isMatchIgnoreStrs) {
                if (this.iteratorStr.startsWith(close, currentIteratorIndex)) {
                    findIndex = currentIteratorIndex;
                    break;
                }

                ++currentIteratorIndex;
            }
        }

        return findIndex;
    }

    public boolean canMove() {
        if (this.iteratorStr == null) {
            return false;
        } else {
            return this.iteratorIndex < this.iteratorStr.length();
        }
    }

    public StringIterator append(CharSequence text) {
        if (this.sb != null) {
            this.sb.append(text);
        }

        return this;
    }

    public StringIterator append(char c) {
        if (this.sb != null) {
            this.sb.append(c);
        }

        return this;
    }

    public StringIterator appendReplacement(CharSequence replacement) {
        if (this.sb == null) {
            this.sb = new StringBuilder(this.iteratorStr.length() + replacement.length());
            if (this.iteratorIndex - this.matchStr.length() > 0) {
                this.sb.append(this.iteratorStr.substring(0, this.iteratorIndex - this.matchStr.length()));
            }
        }

        this.sb.append(replacement);
        return this;
    }

    public String getString() {
        return this.sb == null ? this.iteratorStr : this.sb.toString();
    }

    @Override
    public String toString() {
        return this.iteratorStr.substring(0, this.iteratorIndex) + "->" + this.iteratorStr.substring(this.iteratorIndex);
    }
}