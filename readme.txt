### 大数据相关课程  
[Hadoop 系统入门+核心精讲](https://coding.imooc.com/learn/list/301.html)      
[Python3实战Spark大数据分析及调度](https://coding.imooc.com/learn/list/249.html)

分布式节点部署：  
本机， 虚拟机，云服务器  
用户名，环境路径配置必须一模一样。 最后选了虚拟机，云服务器做尝试。

阿里云ECS服务器：  
新建用户做ssh免密登录，坑好多，做了多方尝试才成功。  
Linux 新建用户
Useradd usename
查看所在组 Groups
设置密码 passwd

出现 -bash-4.1$ 提示时是环境变量有问题。需要复制bash变量
赋予权限 在root用户下，将/home/skel 目录下的这三个文件copy到你的普通用户家目录下
cp -a /etc/skel/.bash* ./username
并更改权限 chmod -R(递归改)777 /home/hadoop
赋予root权限
修改/etc/sudoers文件，找到下面一行，
# Allow root to run any commands anywhere
root ALL=(ALL) ALL
在root下面添加一行，如下所示：
test ALL=(ALL) ALL

切换用户 su username
切换后用户还不能mkdir时查看文件拥有者。需要的话修改拥有者
chown -R user /home/hadoop

改文件权限
chmod go-w /home/user
chmod 700 /home/user/.ssh
chmod 600 /home/user/.ssh/authorized_keys

重启服务
systemctl restart sshd.service

改设置
/etc/ssh/sshd_config
PermitRootLogin no
PasswordAuthentication yes
AllowUsers hadoop
# Change to no to disable s/key passwords
ChallengeResponseAuthentication no

配置hosts，注意公网IP
最后JPS终于显示成功启动2个datanode
但是webUI始终显示一个节点。。云服务器连不上虚拟机。。。


