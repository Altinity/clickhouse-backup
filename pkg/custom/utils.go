package custom

import (
	"bytes"
	"github.com/apex/log"
	"github.com/google/shlex"
	"text/template"
)

func ApplyCommandTemplate(command string, templateData interface{}) []string {
	var b bytes.Buffer
	tpl, err := template.New("").Parse(command)
	if err != nil {
		log.Warnf("custom command template.Parse error: %v", err)
		return []string{command}
	}
	err = tpl.Execute(&b, templateData)
	if err != nil {
		log.Warnf("custom command template.Execute error: %v", err)
		return []string{command}
	}

	args, err := shlex.Split(b.String())
	if err != nil {
		log.Warnf("parse shell command %s error: %v", b.String(), err)
		return []string{command}
	}
	return args
}
