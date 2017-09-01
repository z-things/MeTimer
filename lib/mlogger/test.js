/**
 * Created by jacky on 2015/9/17.
 */
var _ = require('lodash');
function Object (name){
    this.name = name;
    this.intervalId = null;
}
Object.prototype.startSay = function(){
    var self = this;
    var fun = function(self){
        console.log(self.name);
    };
    self.intervalId = setInterval(fun, 2000, self);
};

Object.prototype.stopSay = function(callback){
    var self = this;
    clearInterval(self.intervalId);
    callback();
};

var obj = new Object("jacky");
var obj1 = new Object("jacky1");
var obj2 = new Object("jacky2");
var obj3 = new Object("jacky3");

obj.startSay();
obj1.startSay();
obj2.startSay();
obj3.startSay();

setTimeout(function(){
    obj.stopSay(function(){
        obj = undefined;
        console.log(obj);
    });
    obj1 = undefined;
}, 10000);


