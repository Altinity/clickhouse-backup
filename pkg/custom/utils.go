package custom

import (
	"bytes"
	"github.com/google/shlex"
	"github.com/rs/zerolog/log"
	"text/template"
)

func ApplyCommandTemplate(command string, templateData interface{}) []string {
	var b bytes.Buffer
	tpl, err := template.New("").Parse(command)
	if err != nil {
		log.Warn().Msgf("custom command template.Parse error: %v", err)
		return []string{command}
	}
	err = tpl.Execute(&b, templateData)
	if err != nil {
		log.Warn().Msgf("custom command template.Execute error: %v", err)
		return []string{command}
	}

	args, err := shlex.Split(b.String())
	if err != nil {
		log.Warn().Msgf("parse shell command %s error: %v", b.String(), err)
		return []string{command}
	}
	return args
}
