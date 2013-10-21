package main

import (
	"io"
	"log"
	"os"
	"os/exec"
	"time"
)

type ShellJob struct {
	name         string
	execute      string
	directory    string
	environments []string
	arguments    []string
	logfile      string
	timeout      time.Duration
	expression   string
}

func (self *ShellJob) Run() {
	out, e := os.OpenFile(self.logfile, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0)
	if nil != e {
		log.Println("open log file("+self.logfile+") failed,", e)
		return
	}
	defer out.Close()
	io.WriteString(out, "=============== begin ===============\r\n")
	defer io.WriteString(out, "===============  end  ===============\r\n")

	cmd := exec.Command(self.execute, self.arguments...)
	cmd.Stderr = out
	cmd.Stdin = out
	if nil != self.environments && 0 != len(self.environments) {
		os_env := os.Environ()
		environments := make([]string, 0, len(self.arguments)+len(os_env))
		environments = append(environments, os_env...)
		environments = append(environments, self.environments...)
		cmd.Env = environments
	}

	io.WriteString(out, cmd.Path)
	for idx, s := range cmd.Args {
		if 0 == idx {
			continue
		}
		io.WriteString(out, "\r\n \t\t")
		io.WriteString(out, s)
	}
	io.WriteString(out, "\r\n===============  out  ===============\r\n")

	if e = cmd.Start(); nil != e {
		io.WriteString(out, "start failed, "+e.Error()+"\r\n")
		return
	}
	c := make(chan error, 10)
	go func() {
		c <- cmd.Wait()
	}()

	select {
	case e := <-c:
		if nil != e {
			io.WriteString(out, "run failed, "+e.Error()+"\r\n")
		}
		if nil != cmd.ProcessState {
			io.WriteString(out, "run ok, exit with "+cmd.ProcessState.String()+".\r\n")
		}
	case <-time.After(self.timeout):
		killByPid(cmd.Process.Pid)
		io.WriteString(out, "run timeout, kill it.\r\n")
	}
}
