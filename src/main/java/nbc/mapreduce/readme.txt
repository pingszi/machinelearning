训练数据：
年龄  收入  学生 信用等级 是否买电脑
<=30  high   no   fair       no            
31…40 high   no   fair       yes 
>40   low   yes   fair       yes
>40   low   yes   excellent  no
测试数据
<=30 medium yes   fair        ?

mapper-------------------------------------
key：<=30 medium yes fair  
value：[
        total: 1
        cla: no
        <=30 | no: 1, medium | no: 0, yes | no: 0,fair | no: 1
       ]
context.write(key, value)

key：<=30 medium yes fair  
value：[
        total: 1
	    cla: yes
        <=30 | yes: 0, medium | yes: 0, yes | yes: 0,fair | yes: 1
       ]
context.write(key, value)

key：<=30 medium yes fair  
value：[
        total: 1
        cla: yes
        <=30 | yes: 0, medium | yes: 0, yes | yes: 1,fair | yes: 1
       ]
context.write(key, value)

key：<=30 medium yes fair  
value：[
        total: 1
        cla no
        <=30 | no: 0, medium | no: 0, yes | no: 1,fair | no: 0
       ]
context.write(key, value)
           
reducer-------------------------------------
key：<=30 medium yes fair
value：[
        total: 4
        cla: [no: 2, yes: 2]
        datas:
        [yes:[<=30 | yes: 0, medium | yes: 0, yes | yes 1,fair: | yes: 2]]
	    [no: [<=30 | no: 1, medium | no: 0, yes | no: 1,fair | no: 1]]
	   ]
	   
先验概率P(yes) = 2 / 4   P(no)  = 2 / 4
似然度：P(x|yes) = (0 / 2) * (0 / 2) * (1 / 2) * (2 / 2)  P(x|no)  = (1 / 2) * (0 / 2) * (1 / 2) * (1 / 2)

P(x) = P(yes)P(x|yes) + P(no)P(x|no)
P(yes|x) = P(yes)P(x|yes) / P(x)
P(no|x)  = P(no)P(x|no) / P(x)