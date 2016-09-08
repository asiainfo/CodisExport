package com.asiainfo.codis.conf;

import com.google.gson.annotations.SerializedName;
import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * Created by peng on 16/9/7.
 */
public class Condition{
    public static final String AND = "&&";
    public static final String OR = "||";

    @SerializedName("==")
    private double equal = Double.MAX_VALUE;

    @SerializedName("!=")
    private double notEqual = Double.MAX_VALUE;

    @SerializedName("<=")
    private double lessEqual = Double.MAX_VALUE;

    @SerializedName(">=")
    private double moreEqual = Double.MIN_VALUE;

    @SerializedName("<")
    private double less = Double.MAX_VALUE;

    @SerializedName(">")
    private double more = Double.MIN_VALUE;

    private String like;

    @SerializedName("eq")
    private String eqString;

    @SerializedName("ne")
    private String notEqString;

    private String sign = AND;



    //public abstract boolean matches(String value);

    public double getEqual() {
        return equal;
    }

    public double getNotEqual() {
        return notEqual;
    }

    public double getLessEqual() {
        return lessEqual;
    }

    public double getMoreEqual() {
        return moreEqual;
    }

    public double getLess() {
        return less;
    }

    public double getMore() {
        return more;
    }

    public String getLike() {
        return like;
    }

    public String getEqString() {
        return eqString;
    }

    public String getNotEqString() {
        return notEqString;
    }

    public String getSign() {
        return sign;
    }

    public void setSign(String sign) {
        this.sign = sign;
    }

    @Override
    public String toString() {
        return "Condition{" +
                "equal=" + equal +
                ", notEqual=" + notEqual +
                ", lessEqual=" + lessEqual +
                ", moreEqual=" + moreEqual +
                ", less=" + less +
                ", more=" + more +
                ", like='" + like + '\'' +
                ", eq='" + eqString + '\'' +
                ", notEq='" + notEqString + '\'' +
                ", sign='" + sign + '\'' +
                '}';
    }
}
