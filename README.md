# :oncoming_automobile: AIFFELTHON X SOCAR :oncoming_automobile:

* 프로젝트 기간 : 2022.04.20-2022.06.08
 
* 김빈
* 노지연 : koBERT를 활용한 키워드 추출 모델 구현, 준실시간 데이터 제공을 위한 시스템 자동화 구현 
* 박지호 
* 신혜지
* 홍서영 

# :vertical_traffic_light: Introduction
<쏘카와 관련된 리뷰 및 후기를 기반으로 한 분석 자동화 시스템 구축하기>

최근 방송 뿐만이 아니라 유튜브, 개인 그리고 인플루언서 등의 영향력이 커지고 있습니다. 이는 기업의 이미지에 큰 영향력을 끼치기 때문에 B2C 사업에서는 이에 대한 관리가 필요합니다.
하지만 영향력을 미치는 개인이 많고 동시다발적으로 콘텐츠가 업로드가 되기 때문에 이를 정량적으로 파악하기가 어려울 뿐만 아니라 인력으로 대응하기도 어렵다고 판단했습니다.
이슈에 관한 빠르고 유연한 대처를 위해  다양한 채널에서 쏘카 리뷰들을 수집하고 분석했습니다.

1.	쏘카의 최근 이슈를 파악할 수 있는 급상승 키워드를 제공
2.	쏘카에 대한 인식이 현재 긍정적인지, 부정적인지 알 수 있는 감성분석을 통한 긍/부정 키워드 기획
3.	부정적인 의견을 기반으로 주요 불만 키워드를 파악할 수 있도록 카테고리로 분류
4.	어떤 내용이 부정적인지 정확한 데이터를 제공하고자, 실제 리뷰 데이터 원본 제공

# 🏃 Project Details

* Tools

  Language : Python
  
  IDE : Google Colaboratory
  
  Library : Pytorch
  
  Platform : GCP, Airflow, Kafka, Bigquery, Google Data Studio



[전체 진행과정]
![전체진행흐름도](https://user-images.githubusercontent.com/85794900/172643527-295ab654-3e09-47ec-9b4f-0237a3f95d93.png)

[이진분류]
<img src="https://user-images.githubusercontent.com/85794900/172643828-f8b1bda9-a01f-41d4-91fd-7c4b0f87044e.png" width="1000" height="350"/>


[카테고리 선정 과정]
![카테고라이징방식](https://user-images.githubusercontent.com/85794900/172645592-68f9043c-63a9-4897-84de-2b43b10c0fd6.png)

[시스템 구현도]
![시스템구성도](https://user-images.githubusercontent.com/85794900/172646059-7952dabb-6584-4011-816f-100a306a4de8.png)




# :raising_hand: Results
<br>

<img src="https://user-images.githubusercontent.com/85794900/172743716-cdb6e40b-0911-42bc-979c-c8550424307f.JPG" width="1000" height="600"/>
<img src="https://user-images.githubusercontent.com/85794900/172743788-5efcb201-1f6d-4ad8-b31e-9a54ebdd7f47.JPG" width="1000" height="600"/>


# :love_letter:🕊️
:giraffe: 🧡김빈

:hamster: :yellow_heart: 노지연 : “쏘카에 대한 리뷰 분석”이란 흥미로운 주제를 가지고 처음에는 어떻게 풀어가야 할지 막막했지만, 마음이 맞는 팀원들과 함께 역경에도 굴하지 않고 끝까지 매진하여 좋은 결과를 도출한 만족스런 프로젝트였습니다.

:penguin: :heart: 박지호 : 처음 시작은 

:cat: :green_heart:신혜지

:frog: 💙 홍서영 : 6개월 전 코드 한 줄 몰랐던 제가 멋진 사람들을 만나 충만한 결과물을 내기까지, 하냥 즐거운데다 유익하기까지 한 시간이었습니다. “캐처”팀을 감성분석하면 “Positive” 100%, 키워드 추출하면 “즐거움”이 나올거란 확신이 드네요. 캐처 상사, 퇴사 없어!
