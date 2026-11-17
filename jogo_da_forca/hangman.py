#Módulo hangman.py
#Implementa a lógica pura do jogo (palavra, máscara, vidas)

#==================================================== Anotações ===================================================

#==================================================================================================================

#==================================================== Bibliotecas =================================================

from dataclasses import dataclass
import unicodedata
import re
import hashlib
#===================================================================================================================


#=================================================== Métodos e Classes =============================================

# -- Funções Auxiliares --

#Normaliza a palavra recebida, ela converte as letras maiúsculas em minúsculas, Remover acentos e retornar a palavra normalizada
def normalize_word(word:str)->str:
    normalized = unicodedata.normalize('NFKD', word)
    assent_less_word =  ''.join([s for s in normalized if not unicodedata.combining(s)])

    return assent_less_word.lower()

#verifica se a palavra atende aos requisitos do trabalho
'''

Regras:
- Mínimo de 3 caracteres.
- Máximo de 20 caracteres.
- Somente letras de a até z.
- Não pode conter espaços.
- Não pode conter números.
- Não pode conter hífens ou caracteres especiais.

'''
def validate_word(word: str)->bool:
    return len(word) >= 3 and len(word) <= 20 and bool(re.fullmatch(r'[a-z]+', word))

#calcula o SHA-256 da palavra normalizada.
def calculate_word_hash(word: str) -> str:
    return hashlib.sha256(word.encode('utf-8')).hexdigest()

# -- Estruturas de dados --

#Estrutura de dados que representa o resultado de cada palpite
'''
Atributo	            Significado
is_accepted	            O palpite foi aceito?
is_correct	            O palpite estava correto?
reason	                Motivo da rejeição, caso exista
word_revealed_count	    Quantas letras foram reveladas

'''

@dataclass
class GuessResult:
    is_accepted: bool
    is_correct: bool
    reason: str | None
    word_revealed_count: int 

#Esta classe representa uma partida individual da forca e mantém todas as informações necessárias para executar suas regras
class HangmanGame:    
    def __init__(self, word: str, lives: int = 6):
        '''
                    Atributo	            Tipo	    Finalidade
                    word	                str	        Palavra secreta normalizada
                    initial_lives	        int	        Quantidade inicial de vidas
                    lives	                int	        Vidas restantes
                    correct_letters	        set	        Letras corretas descobertas
                    wrong_letters	        list	    Letras incorretas, na ordem das tentativas
                    used_letters	        set	        Todas as letras já utilizadas
       
         '''

        self.word: str = word
        self.masked_word: str = ''       
        self.initial_lives: int = lives    
        self.remaining_lives: int = lives        
        self.correct_letters: set = set()  
        self.wrong_letters: list = []
        self.used_letters: set = set()

    #Retorna a palavra com as letras descobertas e as letras ocultas.
    def get_masked_word(self) -> str:
        if self.masked_word == self.word:
            return self.masked_word
        
        maskedWord = ''
        for i in range(0,len(self.word)):
            if self.word[i] in self.correct_letters:
                maskedWord = maskedWord + str(self.word[i])
            else:
                maskedWord = maskedWord + "_"
        self.masked_word = maskedWord
        return self.masked_word


    #Verifica e valida o palpite de uma letra de um dos jogadores, retorna uma estrutura com as informacoes da jogada
    def guess_letter(self, letter: str) -> GuessResult:
        jogada = GuessResult(False, False, None, 0)

        if self.is_finished():
            jogada.is_accepted = False
            jogada.is_correct = False
            jogada.reason = "game_not_running"
        else:
            letter_normalize = normalize_word(letter)
            is_accepted_letter = bool(re.fullmatch(r'[a-z]+', letter_normalize))
            if not is_accepted_letter:
                jogada.is_accepted = False
                jogada.is_correct = False
                jogada.reason = "invalid_letter"
            else:
                if is_accepted_letter:
                    if letter_normalize in self.used_letters:
                        jogada.is_accepted = False
                        jogada.is_correct = False
                        jogada.reason = "letter_already_used"
                    else:
                        if letter_normalize in self.word:
                            self.used_letters.add(letter_normalize)
                            self.correct_letters.add(letter_normalize)
                            self.get_masked_word()
                            jogada.is_accepted = True
                            jogada.is_correct = True
                            jogada.reason = None
                        else:
                            self.used_letters.add(letter_normalize)
                            self.wrong_letters.append(letter_normalize)
                            jogada.is_accepted = True
                            jogada.is_correct = False
                            jogada.reason = None
                            self.remaining_lives = self.remaining_lives - 1
                            if(self.remaining_lives < 0):
                                self.remaining_lives = 0
        jogada.word_revealed_count =  len(self.correct_letters)
        return jogada


    #Permite que o jogador tente descobrir a palavra inteira, retorna uma estrutura com informações da jogada
    def guess_word(self, word: str) -> GuessResult:
        jogada = GuessResult(False, False, None, 0)

        if self.is_finished():
            jogada.is_accepted = False
            jogada.is_correct = False
            jogada.reason = "game_not_running"
        else:
            word_normalize = normalize_word(word)
            is_accepted_word = validate_word(word_normalize)
            if not is_accepted_word:
                jogada.is_accepted = False
                jogada.is_correct = False
                jogada.reason = "invalid_word"
            else:
                if is_accepted_word:
                    if word_normalize == self.word:
                        self.masked_word = word
                        self.correct_letters = self.correct_letters | set(word_normalize)
                        jogada.is_accepted = True
                        jogada.is_correct = True
                        jogada.reason = None
                    else:
                        self.remaining_lives = self.remaining_lives - 1
                        if(self.remaining_lives < 0):
                            self.remaining_lives = 0
                        jogada.is_accepted = True
                        jogada.is_correct = False
                        jogada.reason = None
                    
        jogada.word_revealed_count =  len(self.correct_letters)
        return jogada

    #Verifica se todas as letras da palavra foram reveladas
    def is_won(self)->bool:
        return self.masked_word == self.word

    #Verifica se as vidas chegaram a zero
    def is_lost(self)->bool:
        return self.remaining_lives == 0
    
    #Verifica se a partida terminou, independentemente do resultado.
    def is_finished(self):
        return self.is_won() or self.is_lost()

    #Retorna a lista de letras incorretas
    def get_wrong_letters(self) -> list[str]:
        return self.wrong_letters
    #Retorna uma representação do estado atual da forca
    def get_state(self) -> dict:
        
        state_info = {
    
            "masked_word": self.masked_word,
            "wrong_letters": self.wrong_letters,
            "remaining_lives": self.remaining_lives,
            "initial_lives": self.initial_lives,
            "isfinished": self.is_finished(),
            "result": "WON" if self.is_won() else ("LOST" if self.is_lost() else None)
        }

        return state_info


#===================================================================================================================