package com.lxs.flink.kafka;

import java.util.ArrayList;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/5
 **/
public class test {
    public static void main(String[] args) {
        ArrayList<String> tar = new ArrayList<String>();
        String str = "Ｚ｜２６｜ＡFgＺ｜２６｜ＡＺ｜２６｜ＡFgＺ｜２６｜ＡＺ｜２６｜ＡＺ｜２６｜ＡＺ｜２６｜Ａ";
//        String str="Ｚ｜２６｜ＡtrxyrxyrxＺ｜２６｜ＡＺ｜２６｜ＡytcuyvuyviybＺ｜２６｜ＡＺ｜２６｜Ａ ";
        if (str.contains("Ｚ｜２６｜Ａ")) {
            //java中的split方法有两个参数，第一个参数是被分割的字符串，第二个参数则是一个int值，此值默认为0，丢弃末尾空数据。
            //而当第二个参数值大于0时，代表分割字符串后数组的最大长度，当它小于0时，代表获取数组所有值，不会丢弃末尾空值
            String[] arr = str.split("Ｚ｜２６｜Ａ", -1);
            System.out.println(arr.length);
            for (String s : arr) {
                System.out.println("切割后：" + s);
            }
        }
    }
}
