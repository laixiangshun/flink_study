var page = require('webpage').create(),
    system = require('system'),
    t, address;
//写入文件，用来测试。正式版本可以注释掉用来提高速度。
var fs = require("fs");
//读取命令行参数，也就是js文件路径。
if (system.args.length === 1) {
    console.log('Usage: loadspeed.js <some URL>');
//这行代码很重要。凡是结束必须调用。否则phantomjs不会停止
    phantom.exit();
}
page.settings.loadImages = false;  //为了提升加载速度，不加载图片
page.settings.resourceTimeout = 10000;//超过10秒放弃加载
//此处是用来设置截图的参数。不截图没啥用
page.viewportSize = {
    width: 1280,
    height: 800
};
block_urls = ['baidu.com'];//为了提升速度，屏蔽一些需要时间长的。比如百度广告
page.onResourceRequested = function (requestData, request) {
    for (url in block_urls) {
        if (requestData.url.indexOf(block_urls[url]) !== -1) {
            request.abort();
            //console.log(requestData.url + " aborted");
            return;
        }
    }
}
t = Date.now();//看看加载需要多久。
address = system.args[1];
page.open(address, function (status) {
    if (status !== 'success') {
        console.log('FAIL to load the address');
    } else {
        t = Date.now() - t;
//此处原来是为了提取相应的元素。只要可以用document的，还是看可以用。但是自己的无法用document，只能在用字符分割在java里。
        //  var ua = page.evaluate(function() {
        //   return document.getElementById('companyServiceMod').innerHTML;

        // });
        // fs.write("qq.html", ua, 'w');
        // console.log("测试qq: "+ua);
//console.log就是传输回去的内容。
        console.log('Loading time ' + t + ' msec');
        console.log(page.content);
        setTimeout(function () {
            phantom.exit();
        }, 6000);
    }
    phantom.exit();
});
